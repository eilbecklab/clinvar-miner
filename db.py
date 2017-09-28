import sqlite3
from sqlite3 import OperationalError

class DB():
    def __init__(self):
        self.db = sqlite3.connect('clinvar.db', timeout=20)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def and_equals(self, column, value):
        self.query += ' AND ' + column + '=:' + column
        self.parameters[column] = value

    def rows(self):
        return list(map(dict, self.cursor.execute(self.query, self.parameters)))

    def value(self):
        return list(self.cursor.execute(self.query, self.parameters))[0][0]

    def country_name(self, country_code):
        try:
            return list(self.cursor.execute(
                'SELECT submitter_country_name FROM current_submissions WHERE submitter_country_code=? LIMIT 1',
                [country_code]
            ))[0][0]
        except IndexError:
            return None

    def gene_from_rsid(self, rsid):
        try:
            return list(self.cursor.execute(
                'SELECT DISTINCT gene FROM current_submissions WHERE variant_rsid=? LIMIT 1', [rsid]
            ))[0][0]
        except IndexError:
            return None

    def is_gene(self, gene):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE gene=? LIMIT 1', [gene]
        )))

    def is_condition_name(self, condition_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE condition_name=? LIMIT 1', [condition_name]
        )))

    def is_significance(self, significance):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE significance=? LIMIT 1', [significance]
        )))

    def is_variant_name(self, variant_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE variant_name=? LIMIT 1', [variant_name]
        )))

    def max_date(self):
        return list(self.cursor.execute('SELECT date FROM current_submissions LIMIT 1'))[0][0]

    def significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT significance, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submissions
                GROUP BY significance ORDER BY first_seen DESC
            ''')
        ))

    def submissions(self, variant_name = None, min_stars = 0, standardized_method = None, min_conflict_level = 0):
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
                condition1_db AS condition_db,
                condition1_id AS condition_id,
                condition1_name AS condition_name,
                method1 AS method,
                comment1 AS comment
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if variant_name:
            self.and_equals('variant_name', variant_name)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY scv1 ORDER BY submitter_name'

        return self.rows()

    def submitter_id_from_name(self, submitter_name):
        try:
            return list(self.cursor.execute(
                'SELECT submitter_id FROM current_submissions WHERE submitter_name=? LIMIT 1', [submitter_name]
            ))[0][0]
        except IndexError:
            return None

    def submitter_info(self, submitter_id):
        try:
            row = list(self.cursor.execute('''
                SELECT submitter_id, submitter_name, submitter_country_name
                FROM current_submissions WHERE submitter_id=? LIMIT 1
                ''', [submitter_id]
            ))[0]
            return {'id': row[0], 'name': row[1], 'country_name': row[2]}
        except IndexError:
            return None

    def submitter_primary_method(self, submitter_id):
        return list(
            self.cursor.execute('''
                SELECT method FROM current_submissions WHERE submitter_id=?
                GROUP BY method ORDER BY COUNT(*) DESC LIMIT 1
            ''', [submitter_id])
        )[0][0]

    def total_conflicting_variants_by_condition_and_conflict_level(self, min_stars = 0, standardized_method = None):
        self.query = '''
            SELECT
                condition1_db AS condition_db,
                condition1_id AS condition_id,
                condition1_name AS condition_name,
                conflict_level,
                COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=1
        '''

        self.parameters = {
            'min_stars': min_stars,
        }

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY condition_name, conflict_level'

        return list(map(dict, self.cursor.execute(self.query, self.parameters)))

    def total_conflicting_variants_by_conflict_level(self, gene = None, submitter1_id = None, submitter2_id = None,
                                                     min_stars1 = 0, min_stars2 = 0, standardized_method1 = None,
                                                     standardized_method2 = None):
        self.query = '''
            SELECT conflict_level, COUNT(DISTINCT variant_name) AS count FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
        }

        if gene:
            self.and_equals('gene', gene)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if submitter2_id:
            self.and_equals('submitter2_id', submitter2_id)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' GROUP BY conflict_level'

        return self.rows()

    def total_conflicting_variants_by_gene_and_conflict_level(self, min_stars1 = 0, min_stars2 = 0,
                                                              standardized_method1 = None, standardized_method2 = None):
        self.query = '''
            SELECT gene, conflict_level, COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
        }

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' GROUP BY gene, conflict_level'

        return list(map(dict, self.cursor.execute(self.query, self.parameters)))

    def total_conflicting_variants_by_significance_and_significance(self, gene = None, submitter1_id = None,
                                                                    submitter2_id = None, min_stars1 = 0,
                                                                    min_stars2 = 0, standardized_method1 = None,
                                                                    standardized_method2 = None,
                                                                    original_terms = False):
        if original_terms:
            self.query = 'SELECT significance1, significance2'
        else:
            self.query = 'SELECT standardized_significance1 AS significance1, standardized_significance2 AS significance2'

        self.query += ', COUNT(DISTINCT variant_name) AS count FROM current_comparisons'

        self.query += ' WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1'

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
        }

        if gene:
            self.and_equals('gene', gene)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if submitter2_id:
            self.and_equals('submitter2_id', submitter2_id)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        if original_terms:
            self.query += ' GROUP BY significance1, significance2'
        else:
            self.query += ' GROUP BY standardized_significance1, standardized_significance2'

        return self.rows()

    def total_conflicting_variants_by_submitter_and_conflict_level(self, submitter1_id = None, min_stars1 = 0,
                                                                   min_stars2 = 0, standardized_method1 = None,
                                                                   standardized_method2 = None):

        if submitter1_id:
            self.query = 'SELECT submitter2_id AS submitter_id'
        else:
            self.query = 'SELECT submitter1_id AS submitter_id'

        self.query += '''
            , conflict_level, COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
        }

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' GROUP BY submitter_id, conflict_level'

        return self.rows()

    def total_significance_terms_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('SELECT date, COUNT(DISTINCT significance) AS count FROM submissions GROUP BY date')
        ))

    def total_submissions(self):
        return list(self.cursor.execute('SELECT COUNT(*) FROM current_submissions'))[0][0]

    def total_submissions_by_country(self, min_stars = 0, standardized_method = None, min_conflict_level = 0):
        self.query = '''
            SELECT
                submitter1_country_code AS country_code,
                submitter1_country_name AS country_name,
                COUNT(DISTINCT scv1) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY country_code ORDER BY count DESC'

        return self.rows()

    def total_submissions_by_method(self, min_stars = 0, min_conflict_level = 0):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT method1 AS method, COUNT(DISTINCT scv1) AS count
                    FROM current_comparisons
                    WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
                    GROUP BY method ORDER BY count DESC
                ''',
                {
                    'min_stars': min_stars,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_submissions_by_standardized_method_over_time(self, min_stars = 0, min_conflict_level = 0):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT date, standardized_method1 AS standardized_method, COUNT(DISTINCT scv1) AS count
                    FROM comparisons
                    WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
                    GROUP BY date, standardized_method ORDER BY date, count DESC
                ''',
                {
                    'min_stars': min_stars,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_submissions_by_submitter(self, country_code = None, min_stars = 0, standardized_method = None,
                                       min_conflict_level = 0):
        self.query = '''
            SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name, COUNT(DISTINCT scv1) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if country_code != None:
            self.and_equals('submitter1_country_code', country_code)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)

        self.query += ' GROUP BY submitter1_id ORDER BY count DESC'

        return self.rows()

    def total_variants(self, gene = None, condition1_name = None, submitter1_id = None, submitter2_id = None,
                       significance1 = None, min_stars1 = 0, min_stars2 = 0, standardized_method1 = None,
                       standardized_method2 = None, min_conflict_level = 0, original_terms = False):
        self.query = '''
            SELECT COUNT(DISTINCT variant_name) FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if condition1_name:
            self.and_equals('condition1_name', condition1_name)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if submitter2_id:
            self.and_equals('submitter2_id', submitter2_id)

        if significance1:
            if original_terms:
                self.and_equals('significance1', significance1)
            else:
                self.and_equals('standardized_significance1', significance1)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        return self.value()

    def total_variants_by_condition(self, gene = None, significance1 = None, submitter1_id = 0, min_stars = 0,
                                    standardized_method = None, min_conflict_level = 0, original_terms = False):
        self.query = '''
            SELECT
                condition1_db AS condition_db, condition1_id AS condition_id, condition1_name AS condition_name,
                COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if significance1:
            if original_terms:
                self.and_equals('significance1', significance1)
            else:
                self.and_equals('standardized_significance1', significance1)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY condition_name ORDER BY count DESC'

        return list(map(dict, self.cursor.execute(self.query, self.parameters)))

    def total_variants_by_condition_and_significance(self, gene = None, submitter_id = None, min_stars = 0,
                                                     standardized_method = None, min_conflict_level = 0,
                                                     original_terms = False):
        self.query = '''
            SELECT
                condition1_name AS condition_name,
                COUNT(DISTINCT variant_name) AS count
        '''

        if original_terms:
            self.query += ', significance1 AS significance'
        else:
            self.query += ', standardized_significance1 AS significance'

        self.query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if submitter_id:
            self.and_equals('submitter1_id', submitter_id)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)

        self.query += ' GROUP BY condition_name, significance'

        return self.rows()

    def total_variants_by_gene(self, condition1_name = None, submitter1_id = 0, significance1 = None, min_stars1 = 0,
                               min_stars2 = 0, standardized_method1 = None, standardized_method2 = None,
                               min_conflict_level = 0, original_terms = False):
        self.query = '''
            SELECT gene, COUNT(DISTINCT variant_name) AS count FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
            'min_conflict_level': min_conflict_level,
        }

        if condition1_name:
            self.and_equals('condition1_name', condition1_name)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if significance1:
            if original_terms:
                self.and_equals('significance1', significance1)
            else:
                self.and_equals('standardized_significance1', significance1)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' GROUP BY gene ORDER BY count DESC'

        return self.rows()

    def total_variants_by_gene_and_significance(self, condition_name = None, submitter_id = None, min_stars = 0,
                                                standardized_method = None, min_conflict_level = 0,
                                                original_terms = False):
        self.query = 'SELECT gene, COUNT(DISTINCT variant_name) AS count'

        if original_terms:
            self.query += ', significance1 AS significance'
        else:
            self.query += ', standardized_significance1 AS significance'

        self.query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if condition_name:
            self.and_equals('condition1_name', condition_name)

        if submitter_id:
            self.and_equals('submitter1_id', submitter_id)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY gene, significance'

        return self.rows()

    def total_variants_by_significance(self, gene = None, condition_name = None, submitter_id = None, min_stars = 0,
                                       standardized_method = None, min_conflict_level = 0, original_terms = False):
        self.query = 'SELECT COUNT(DISTINCT variant_name) AS count'

        if original_terms:
            self.query += ', significance1 AS significance'
        else:
            self.query += ', standardized_significance1 AS significance'

        self.query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if gene:
            self.and_equals('gene', gene)

        if condition_name:
            self.and_equals('condition1_name', condition_name)

        if submitter_id:
            self.and_equals('submitter1_id', submitter_id)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY significance ORDER BY count DESC'

        return self.rows()

    def total_variants_by_submitter(self, gene = None, condition1_name = None, submitter1_id = None,
                                    significance1 = None, min_stars1 = 0, min_stars2 = 0, standardized_method1 = None,
                                    standardized_method2 = None, min_conflict_level = 0, original_terms = False):
        if submitter1_id:
            self.query = 'SELECT submitter2_id AS submitter_id, submitter2_name AS submitter_name'
        else:
            self.query = 'SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name'

        self.query += '''
            , COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if condition1_name:
            self.and_equals('condition1_name', condition1_name)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if significance1:
            if original_terms:
                self.and_equals('significance1', significance1)
            else:
                self.and_equals('standardized_significance1', significance1)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' GROUP BY submitter_id ORDER BY count DESC'

        return self.rows()

    def total_variants_by_submitter_and_significance(self, gene = None, condition_name = None, min_stars = 0,
                                                     standardized_method = None, min_conflict_level = 0,
                                                     original_terms = False):
        self.query = 'SELECT submitter1_id AS submitter_id, COUNT(DISTINCT variant_name) AS count'

        if original_terms:
            self.query += ', significance1 AS significance'
        else:
            self.query += ', standardized_significance1 AS significance'

        self.query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars': min_stars,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if condition_name:
            self.and_equals('condition1_name', condition_name)

        if standardized_method:
            self.and_equals('standardized_method1', standardized_method)
            self.and_equals('standardized_method2', standardized_method)

        self.query += ' GROUP BY submitter_id, significance'

        return self.rows()

    def condition_info(self, condition_name):
        try:
            #prefer a row that links the condition name to a condition ID
            row = list(self.cursor.execute('''
                SELECT condition_db, condition_id FROM current_submissions WHERE condition_name=?
                GROUP BY condition_id=='' ORDER BY condition_id=='' LIMIT 1
            ''', [condition_name]))[0]
            return {'db': row[0], 'id': row[1], 'name': condition_name}
        except IndexError:
            return None

    def variant_info(self, variant_name):
        try:
            row = list(self.cursor.execute(
                'SELECT variant_id, variant_rsid FROM current_submissions WHERE variant_name=? LIMIT 1', [variant_name]
            ))[0]
            return {'id': row[0], 'name': variant_name, 'rsid': row[1]}
        except IndexError:
            return None

    def variant_name_from_rcv(self, rcv):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM current_submissions WHERE rcv=? LIMIT 1', [rcv]
            ))[0][0]
        except IndexError:
            return None

    def variant_name_from_rsid(self, rsid):
        rows = list(self.cursor.execute(
            'SELECT DISTINCT variant_name FROM current_submissions WHERE variant_rsid=?', [rsid]
        ))
        return rows[0][0] if len(rows) == 1 else None

    def variant_name_from_scv(self, scv):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM current_submissions WHERE scv=? LIMIT 1', [scv]
            ))[0][0]
        except IndexError:
            return None

    def variants(self, gene = None, condition1_name = None, submitter1_id = None, submitter2_id = None,
                 significance1 = None, significance2 = None, min_stars1 = 0, min_stars2 = 0,
                 standardized_method1 = None, standardized_method2 = None, min_conflict_level = 1,
                 original_terms = False):
        self.query = '''
            SELECT DISTINCT variant_name, variant_rsid FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        self.parameters = {
            'min_stars1': min_stars1,
            'min_stars2': min_stars2,
            'min_conflict_level': min_conflict_level,
        }

        if gene != None:
            self.and_equals('gene', gene)

        if condition1_name:
            self.and_equals('condition1_name', condition1_name)

        if submitter1_id:
            self.and_equals('submitter1_id', submitter1_id)

        if submitter2_id:
            self.and_equals('submitter2_id', submitter2_id)

        if significance1:
            if original_terms:
                self.and_equals('significance1', significance1)
            else:
                self.and_equals('standardized_significance1', significance1)

        if significance2:
            if original_terms:
                self.and_equals('significance2', significance2)
            else:
                self.and_equals('standardized_significance2', significance2)

        if standardized_method1:
            self.and_equals('standardized_method1', standardized_method1)

        if standardized_method2:
            self.and_equals('standardized_method2', standardized_method2)

        self.query += ' ORDER BY variant_name'

        return self.rows()
