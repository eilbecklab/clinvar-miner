import sqlite3
from asynchelper import promise
from sqlite3 import OperationalError

class DB():
    def __init__(self):
        self.db = sqlite3.connect('clinvar.db', timeout=20, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def and_equals(self, column, value):
        if type(value) == list:
            if not value:
                return
            db_types = {
                int: 'INTEGER',
                str: 'TEXT',
            }
            self.cursor.execute('DROP TABLE IF EXISTS ' + column) #the same connection may perform multiple queries
            self.cursor.execute('CREATE TEMP TABLE ' + column + ' (value ' + db_types[type(value[0])] + ')')
            self.cursor.execute('CREATE INDEX ' + column + '__value ON ' + column + ' (value)')
            self.cursor.executemany('INSERT INTO ' + column + ' VALUES (?)', map(lambda value: [value], value))
            self.query += ' AND ' + column + ' IN (SELECT value FROM ' + column + ')'
        else:
            self.query += ' AND ' + column + '=:' + column
            self.parameters[column] = value

    def rows(self):
        return list(map(dict, self.cursor.execute(self.query, self.parameters)))

    def value(self):
        return list(self.cursor.execute(self.query, self.parameters))[0][0]

    def clinvar_names_from_mondo_id(self, mondo_id, date = None):
        try:
            self.cursor.execute(
                'SELECT clinvar_name FROM mondo_clinvar_relationships WHERE mondo_id=? AND date=?',
                [mondo_id, date or self.max_date()]
            )
            return [row[0] for row in self.cursor.fetchall()]
        except IndexError:
            return []

    def condition_xrefs(self, condition_name, date = None):
        try:
            return list(self.cursor.execute(
                '''
                    SELECT DISTINCT condition_xrefs FROM submissions WHERE condition_name=? AND date=?
                    ORDER BY condition_xrefs=='' /* prefer a row that has cross-references */ LIMIT 1
                ''',
                [condition_name, date or self.max_date()]
            ))[0][0].split(';')
        except IndexError:
            return []

    def country_name(self, country_code, date = None):
        try:
            return list(self.cursor.execute(
                'SELECT submitter_country_name FROM submissions WHERE submitter_country_code=? AND date=? LIMIT 1',
                [country_code, date or self.max_date()]
            ))[0][0]
        except IndexError:
            return None

    def dates(self):
        return list(map(
            lambda row: row[0],
            self.cursor.execute('SELECT DISTINCT date FROM submissions ORDER BY date DESC')
        ))

    def gene_from_rsid(self, rsid, date = None):
        try:
            return list(self.cursor.execute(
                'SELECT DISTINCT gene FROM submissions WHERE rsid=? AND date=? LIMIT 1',
                [rsid, date or self.max_date()]
            ))[0][0]
        except IndexError:
            return None

    def gene_info(self, gene, original_genes, date = None):
        try:
            if original_genes:
                query = 'SELECT gene_type FROM submissions WHERE gene=? AND date=? LIMIT 1'
            else:
                query = 'SELECT normalized_gene_type FROM submissions WHERE normalized_gene=? AND date=? LIMIT 1'
            ret = {'name': gene, 'type': list(self.cursor.execute(query, [gene, date or self.max_date()]))[0][0]}
        except IndexError:
            ret = {'name': gene, 'type': 0}

        if not date or date == self.max_date():
            if original_genes:
                query = 'SELECT see_also FROM gene_links WHERE gene=?'
            else:
                query = 'SELECT see_also FROM normalized_gene_links WHERE gene=?'
            ret['see_also'] = list(map(lambda row: row[0], self.cursor.execute(query, [gene])))
        else:
            ret['see_also'] = []

        return ret

    def is_condition_name(self, condition_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE condition_name=? LIMIT 1',
            [condition_name]
        )))

    def is_date(self, date):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE date=? LIMIT 1',
            [date]
        )))

    def is_gene(self, gene):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE gene=? OR normalized_gene=? LIMIT 1',
            [gene, gene]
        )))

    def is_method(self, method):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE method=? OR normalized_method=? LIMIT 1',
            [method, method]
        )))

    def is_mondo_condition_id(self, mondo_condition_id):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM mondo_clinvar_relationships WHERE mondo_id=? LIMIT 1',
            [mondo_condition_id]
        )))

    def is_significance(self, significance):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE significance=? LIMIT 1',
            [significance]
        )))

    def is_submitter_id(self, submitter_id):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE submitter_id=? LIMIT 1',
            [submitter_id]
        )))

    def is_variant_name(self, variant_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM submissions WHERE variant_name=? LIMIT 1',
            [variant_name]
        )))

    def max_date(self):
        return list(self.cursor.execute('SELECT MAX(date) FROM submissions'))[0][0]

    def significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT significance, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submissions
                GROUP BY significance ORDER BY last_seen DESC, first_seen DESC
            ''')
        ))

    def submissions(self, **kwargs):
        self.query = '''
            SELECT
                variant_name,
                submitter1_id AS submitter_id,
                submitter1_name AS submitter_name,
                rcv1 AS rcv,
                scv1 AS scv,
                significance1 AS significance,
                last_eval1 AS last_eval,
                review_status1 AS review_status,
                condition1_name AS condition_name,
                method1 AS method,
                comment1 AS comment
            FROM comparisons
            WHERE
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars': kwargs.get('min_stars', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('variant_name'):
            self.and_equals('variant_name', kwargs['variant_name'])

        if kwargs.get('normalized_method'):
            self.and_equals('normalized_method1', kwargs['normalized_method'])
            self.and_equals('normalized_method2', kwargs['normalized_method'])

        self.query += ' GROUP BY scv1 ORDER BY submitter_name'

        return self.rows()

    def submitter_id_from_name(self, submitter_name, date = None):
        try:
            return list(self.cursor.execute(
                'SELECT submitter_id FROM submissions WHERE submitter_name=? AND date=? LIMIT 1',
                [submitter_name, date or self.max_date()]
            ))[0][0]
        except IndexError:
            return None

    def submitter_info(self, submitter_id, date = None):
        try:
            row = list(self.cursor.execute(
                '''
                    SELECT submitter_name, submitter_country_name
                    FROM submissions WHERE submitter_id=? AND date=? LIMIT 1
                ''',
                [submitter_id, date or self.max_date()]
            ))[0]
            return {'id': submitter_id, 'name': row[0], 'country_name': row[1]}
        except IndexError:
            return {'id': submitter_id, 'name': str(submitter_id), 'country_name': ''}

    def submitter_primary_method(self, submitter_id, date = None):
        try:
            return list(
                self.cursor.execute('''
                    SELECT method FROM submissions WHERE submitter_id=? AND date=?
                    GROUP BY method ORDER BY COUNT(*) DESC LIMIT 1
                ''', [submitter_id, date or self.max_date()])
            )[0][0]
        except IndexError:
            return 'not provided'

    def total_conditions(self, **kwargs):
        self.query = '''
            SELECT COUNT(DISTINCT condition1_name) FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene'):
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        return self.value()

    def total_genes(self, **kwargs):
        if kwargs.get('original_genes'):
            self.query = 'SELECT COUNT(DISTINCT gene) FROM comparisons'
        else:
            self.query = 'SELECT COUNT(DISTINCT normalized_gene) FROM comparisons'

        self.query += '''
            WHERE
                star_level1>=:min_stars1
                AND star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        return self.value()

    def mondo_conditions(self, date = None):
        return list(map(dict, self.cursor.execute(
            'SELECT DISTINCT mondo_id, mondo_name FROM mondo_clinvar_relationships WHERE date=? ORDER BY mondo_name',
            [date or self.max_date()]
        )))

    def mondo_name(self, mondo_id, date = None):
        self.cursor.execute(
            'SELECT mondo_name FROM mondo_clinvar_relationships where mondo_id=? AND date=?',
            [mondo_id, date or self.max_date()]
        )
        name = self.cursor.fetchone()[0]
        return name

    @promise
    def total_significance_terms_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('SELECT date, COUNT(DISTINCT significance) AS count FROM submissions GROUP BY date')
        ))

    def total_submissions(self, **kwargs):
        self.query = '''
            SELECT COUNT(DISTINCT scv1) FROM comparisons
            WHERE
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level
                AND date=:date
        '''

        self.parameters = {
            'min_stars': kwargs.get('min_stars', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('country_code'):
            self.and_equals('submitter1_country_code', kwargs['country_code'])

        if kwargs.get('normalized_method'):
            self.and_equals('normalized_method1', kwargs['normalized_method'])

        return self.value()

    def total_submitters(self, **kwargs):
        self.query = '''
            SELECT COUNT(DISTINCT submitter1_id) FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene'):
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        return self.value()

    def total_submissions_by_country(self, **kwargs):
        self.query = '''
            SELECT
                submitter1_country_code AS country_code,
                submitter1_country_name AS country_name,
                COUNT(DISTINCT scv1) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars': kwargs.get('min_stars', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('normalized_method'):
            self.and_equals('normalized_method1', kwargs['normalized_method'])
            self.and_equals('normalized_method2', kwargs['normalized_method'])

        self.query += ' GROUP BY country_code ORDER BY count DESC'

        return self.rows()

    def total_submissions_by_method(self, **kwargs):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT method1 AS method, COUNT(DISTINCT scv1) AS count
                    FROM comparisons
                    WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
                    GROUP BY method ORDER BY count DESC
                ''',
                {
                    'min_stars': kwargs.get('min_stars', 0),
                    'min_conflict_level': kwargs.get('min_conflict_level', -1),
                    'date': kwargs.get('date') or self.max_date(),
                }
            )
        ))

    def total_submissions_by_normalized_method_over_time(self, **kwargs):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT date, normalized_method1 AS normalized_method, COUNT(DISTINCT scv1) AS count
                    FROM comparisons
                    WHERE
                        star_level1>=:min_stars AND
                        star_level2>=:min_stars AND
                        conflict_level>=:min_conflict_level AND
                        date<=:date
                    GROUP BY date, normalized_method ORDER BY date, count DESC
                ''',
                {
                    'min_stars': kwargs.get('min_stars', 0),
                    'min_conflict_level': kwargs.get('min_conflict_level', -1),
                    'date': kwargs.get('date') or self.max_date(),
                }
            )
        ))

    def total_submissions_by_submitter(self, **kwargs):
        self.query = '''
            SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name, COUNT(DISTINCT scv1) AS count
            FROM comparisons
            WHERE star_level1>=:min_stars AND conflict_level>=:min_conflict_level AND date=:date
        '''

        self.parameters = {
            'min_stars': kwargs.get('min_stars', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('country_code'):
            self.and_equals('submitter1_country_code', kwargs['country_code'])

        if kwargs.get('normalized_method'):
            self.and_equals('normalized_method1', kwargs['normalized_method'])

        self.query += ' GROUP BY submitter1_id ORDER BY count DESC'

        return self.rows()

    def total_variants(self, **kwargs):
        self.query = '''
            SELECT COUNT(DISTINCT variant_name) FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('submitter2_id'):
            self.and_equals('submitter2_id', kwargs['submitter2_id'])

        if kwargs.get('significance1'):
            if kwargs.get('original_terms'):
                self.and_equals('significance1', kwargs['significance1'])
            else:
                self.and_equals('normalized_significance1', kwargs['significance1'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        return self.value()

    @promise
    def total_variants_by_condition(self, **kwargs):
        if type(kwargs.get('condition1_name')) is not str:
            self.query = 'SELECT condition1_name AS condition_name'
        else:
            self.query = 'SELECT condition2_name AS condition_name'

        if kwargs.get('original_genes'):
            self.query += ', COUNT(DISTINCT gene) AS gene_count'
        else:
            self.query += ', COUNT(DISTINCT normalized_gene) AS gene_count'

        self.query += '''
            , COUNT(DISTINCT submitter1_id) AS submitter_count
            , COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('significance1'):
            if kwargs.get('original_terms'):
                self.and_equals('significance1', kwargs['significance1'])
            else:
                self.and_equals('normalized_significance1', kwargs['significance1'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY condition_name ORDER BY count DESC'

        return self.rows()

    @promise
    def total_variants_by_condition_and_significance(self, **kwargs):
        self.query = 'SELECT condition1_name AS condition_name, COUNT(DISTINCT variant_name) AS count'

        if kwargs.get('original_terms'):
            self.query += ', significance1 AS significance'
        else:
            self.query += ', normalized_significance1 AS significance'

        self.query += '''
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY condition_name, significance'

        return self.rows()

    @promise
    def total_variants_by_gene(self, **kwargs):
        if kwargs.get('original_genes'):
            self.query = 'SELECT gene'
        else:
            self.query = 'SELECT normalized_gene AS gene'

        self.query += '''
            , COUNT(DISTINCT condition1_name) AS condition_count
            , COUNT(DISTINCT submitter1_id) AS submitter_count
            , COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('significance1'):
            if kwargs.get('original_terms'):
                self.and_equals('significance1', kwargs['significance1'])
            else:
                self.and_equals('normalized_significance1', kwargs['significance1'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        if kwargs.get('gene'):
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('original_genes'):
            self.query += ' GROUP BY gene ORDER BY count DESC'
        else:
            self.query += ' GROUP BY normalized_gene ORDER BY count DESC'

        return self.rows()

    @promise
    def total_variants_by_gene_and_significance(self, **kwargs):
        if kwargs.get('original_genes'):
            self.query = 'SELECT gene'
        else:
            self.query = 'SELECT normalized_gene AS gene'

        self.query += ', COUNT(DISTINCT variant_name) AS count'

        if kwargs.get('original_terms'):
            self.query += ', significance1 AS significance'
        else:
            self.query += ', normalized_significance1 AS significance'

        self.query += '''
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        if kwargs.get('original_genes'):
            self.query += ' GROUP BY gene, significance'
        else:
            self.query += ' GROUP BY normalized_gene, significance'

        return self.rows()

    @promise
    def total_variants_by_significance(self, **kwargs):
        self.query = 'SELECT COUNT(DISTINCT variant_name) AS count'

        if kwargs.get('original_terms'):
            self.query += ', significance1 AS significance'
        else:
            self.query += ', normalized_significance1 AS significance'

        if kwargs.get('original_genes'):
            self.query += ', COUNT(DISTINCT gene) AS gene_count'
        else:
            self.query += ', COUNT(DISTINCT normalized_gene) AS gene_count'

        self.query += '''
            , COUNT(DISTINCT condition1_name) AS condition_count
            , COUNT(DISTINCT submitter1_id) AS submitter_count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY significance ORDER BY count DESC'

        return self.rows()

    @promise
    def total_variants_by_submitter(self, **kwargs):
        if type(kwargs.get('submitter1_id')) is not int:
            self.query = 'SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name'
        else:
            self.query = 'SELECT submitter2_id AS submitter_id, submitter2_name AS submitter_name'

        if kwargs.get('original_genes'):
            self.query += ', COUNT(DISTINCT gene) AS gene_count'
        else:
            self.query += ', COUNT(DISTINCT normalized_gene) AS gene_count'

        self.query += '''
            , COUNT(DISTINCT condition1_name) AS condition_count
            , COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('significance1'):
            if kwargs.get('original_terms'):
                self.and_equals('significance1', kwargs['significance1'])
            else:
                self.and_equals('normalized_significance1', kwargs['significance1'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        if kwargs.get('submitter_ids'):
            self.and_equals('submitter_id', kwargs['submitter_ids'])

        self.query += ' GROUP BY submitter_id ORDER BY count DESC'

        return self.rows()

    @promise
    def total_variants_by_submitter_and_significance(self, **kwargs):
        self.query = 'SELECT submitter1_id AS submitter_id, COUNT(DISTINCT variant_name) AS count'

        if kwargs.get('original_terms'):
            self.query += ', significance1 AS significance'
        else:
            self.query += ', normalized_significance1 AS significance'

        self.query += '''
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY submitter_id, significance'

        return self.rows()


    @promise
    def total_variants_in_conflict_by_condition_and_conflict_level(self, **kwargs):
        if type(kwargs.get('condition1_name')) is not str:
            self.query = 'SELECT condition1_name AS condition_name'
        else:
            self.query = 'SELECT condition2_name AS condition_name'

        self.query += '''
            , conflict_level, COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', 1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        self.query += ' GROUP BY condition_name, conflict_level'

        return self.rows()

    @promise
    def total_variants_in_conflict_by_conflict_level(self, **kwargs):
        self.query = '''
            SELECT conflict_level, COUNT(DISTINCT variant_name) AS count FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', 1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('submitter2_id'):
            self.and_equals('submitter2_id', kwargs['submitter2_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY conflict_level'

        return self.rows()

    @promise
    def total_variants_in_conflict_by_gene_and_conflict_level(self, **kwargs):
        if kwargs.get('original_genes'):
            self.query = 'SELECT gene'
        else:
            self.query = 'SELECT normalized_gene AS gene'

        self.query += '''
            , conflict_level, COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', 1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        if kwargs.get('gene'):
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('original_genes'):
            self.query += ' GROUP BY gene, conflict_level'
        else:
            self.query += ' GROUP BY normalized_gene, conflict_level'

        return self.rows()

    @promise
    def total_variants_in_conflict_by_significance_and_significance(self, **kwargs):
        if kwargs.get('original_terms'):
            self.query = 'SELECT significance1, significance2'
        else:
            self.query = '''
                SELECT normalized_significance1 AS significance1, normalized_significance2 AS significance2
            '''

        self.query += ', conflict_level, COUNT(DISTINCT variant_name) AS count FROM comparisons'

        self.query += '''
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', 1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('submitter2_id'):
            self.and_equals('submitter2_id', kwargs['submitter2_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        if kwargs.get('original_terms'):
            self.query += ' GROUP BY significance1, significance2'
        else:
            self.query += ' GROUP BY normalized_significance1, normalized_significance2'

        return self.rows()

    @promise
    def total_variants_in_conflict_by_submitter_and_conflict_level(self, **kwargs):
        if type(kwargs.get('submitter1_id')) is not int:
            self.query = 'SELECT submitter1_id AS submitter_id'
        else:
            self.query = 'SELECT submitter2_id AS submitter_id'

        self.query += '''
            , conflict_level, COUNT(DISTINCT variant_name) AS count
            FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', 1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        self.query += ' GROUP BY submitter_id, conflict_level'

        return self.rows()

    def total_variants_without_significance(self, **kwargs):
        self.query = '''
            SELECT COUNT(DISTINCT variant_name) FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
            'significance': kwargs['significance'],
        }

        if kwargs.get('original_terms'):
            self.query += ' AND significance1!=:significance AND significance2!=:significance'
        else:
            self.query += ' AND normalized_significance1!=:significance AND normalized_significance2!=:significance'

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        return self.value()

    def variant_info(self, variant_name, date = None):
        try:
            row = list(self.cursor.execute(
                'SELECT variant_id, rsid FROM submissions WHERE variant_name=? AND date=? LIMIT 1',
                [variant_name, date or self.max_date()]
            ))[0]
            return {'id': row[0], 'name': variant_name, 'rsid': row[1]}
        except IndexError:
            return {'id': variant_name, 'name': variant_name, 'rsid': ''}

    def variant_name_from_rcv(self, rcv, date = None):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM submissions WHERE rcv=? AND date=? LIMIT 1',
                [rcv, date or self.max_date()]
            ))[0][0]
        except IndexError:
            return None

    def variant_name_from_rsid(self, rsid, date = None):
        rows = list(self.cursor.execute(
            'SELECT DISTINCT variant_name FROM submissions WHERE rsid=? AND date=?',
            [rsid, date or self.max_date()]
        ))
        return rows[0][0] if len(rows) == 1 else None

    def variant_name_from_scv(self, scv, date = None):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM submissions WHERE scv=? AND date=? LIMIT 1',
                [scv, date or self.max_date()]
            ))[0][0]
        except IndexError:
            return None

    @promise
    def variants(self, **kwargs):
        self.query = '''
            SELECT variant_name, rsid FROM comparisons
            WHERE
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=:min_conflict_level AND
                date=:date
        '''

        self.parameters = {
            'min_stars1': kwargs.get('min_stars1', 0),
            'min_stars2': kwargs.get('min_stars2', 0),
            'min_conflict_level': kwargs.get('min_conflict_level', -1),
            'date': kwargs.get('date') or self.max_date(),
        }

        if kwargs.get('gene') != None:
            if kwargs.get('original_genes'):
                self.and_equals('gene', kwargs['gene'])
            else:
                self.and_equals('normalized_gene', kwargs['gene'])

        if kwargs.get('condition1_name'):
            self.and_equals('condition1_name', kwargs['condition1_name'])

        if kwargs.get('submitter1_id'):
            self.and_equals('submitter1_id', kwargs['submitter1_id'])

        if kwargs.get('submitter2_id'):
            self.and_equals('submitter2_id', kwargs['submitter2_id'])

        if kwargs.get('significance1'):
            if kwargs.get('original_terms'):
                self.and_equals('significance1', kwargs['significance1'])
            else:
                self.and_equals('normalized_significance1', kwargs['significance1'])

        if kwargs.get('significance2'):
            if kwargs.get('original_terms'):
                self.and_equals('significance2', kwargs['significance2'])
            else:
                self.and_equals('normalized_significance2', kwargs['significance2'])

        if kwargs.get('normalized_method1'):
            self.and_equals('normalized_method1', kwargs['normalized_method1'])

        if kwargs.get('normalized_method2'):
            self.and_equals('normalized_method2', kwargs['normalized_method2'])

        if kwargs.get('gene_type', -1) != -1:
            if kwargs.get('original_genes'):
                self.and_equals('gene_type', kwargs['gene_type'])
            else:
                self.and_equals('normalized_gene_type', kwargs['gene_type'])

        self.query += ' GROUP BY variant_name ORDER BY variant_name'

        return self.rows()
