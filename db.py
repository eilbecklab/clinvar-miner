import sqlite3
from sqlite3 import OperationalError

class DB():
    def __init__(self):
        self.db = sqlite3.connect('clinvar.db', timeout=20)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def country_name(self, country_code):
        try:
            return list(self.cursor.execute(
                'SELECT submitter_country_name FROM current_submissions WHERE submitter_country_code=? LIMIT 1',
                [country_code]
            ))[0][0]
        except IndexError:
            return country_code

    def is_gene(self, gene):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE gene=? LIMIT 1', [gene]
        )))

    def is_trait_name(self, trait_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE trait_name=? LIMIT 1', [trait_name]
        )))

    def is_variant_name(self, variant_name):
        return bool(list(self.cursor.execute(
            'SELECT 1 FROM current_submissions WHERE variant_name=? LIMIT 1', [variant_name]
        )))

    def max_date(self):
        return list(self.cursor.execute('SELECT MAX(date) FROM submissions'))[0][0]

    def old_significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT * FROM (
                    SELECT significance, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submissions
                    GROUP BY significance ORDER BY significance
                ) WHERE last_seen!=(SELECT MAX(date) FROM submissions)
            ''')
        ))

    def significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT significance, MIN(date) AS first_seen FROM submissions
                GROUP BY significance ORDER BY significance
            ''')
        ))

    def submissions(self, gene = None, variant_name = None, min_stars = 0, standardized_method = None,
                    min_conflict_level = 0):
        query = '''
            SELECT
                variant_name,
                gene,
                submitter1_id AS submitter_id,
                submitter1_name AS submitter_name,
                rcv1 AS rcv,
                scv1 AS scv,
                significance1 AS significance,
                last_eval1 AS last_eval,
                review_status1 AS review_status,
                trait1_db AS trait_db,
                trait1_id AS trait_id,
                trait1_name AS trait_name,
                method1 AS method,
                comment1 AS comment
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if gene != None:
            query += ' AND gene=:gene'

        if variant_name:
            query += ' AND variant_name=:variant_name'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY scv1 ORDER BY submitter_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'variant_name': variant_name,
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def submitter_info(self, submitter_id):
        try:
            row = list(self.cursor.execute('''
                SELECT submitter_id, submitter_name, submitter_country_name
                FROM current_submissions WHERE submitter_id=? LIMIT 1
                ''', [submitter_id]
            ))[0]
            return {'id': row[0], 'name': row[1], 'country_name': row[2]}
        except IndexError:
            return {'id': submitter_id, 'name': str(submitter_id)}

    def submitter_primary_method(self, submitter_id):
        return list(
            self.cursor.execute('''
                SELECT method FROM current_submissions WHERE submitter_id=?
                GROUP BY method ORDER BY COUNT(*) DESC LIMIT 1
            ''', [submitter_id])
        )[0][0]

    def total_conflicting_variants_by_conflict_level(self, submitter1_id = None, submitter2_id = None, min_stars1 = 0,
                                                     min_stars2 = 0, standardized_method1 = None,
                                                     standardized_method2 = None):
        query = '''
            SELECT conflict_level, COUNT(DISTINCT variant_name) AS count FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1
        '''

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        query += ' GROUP BY conflict_level ORDER BY conflict_level'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                }
            )
        ))

    def total_conflicting_variants_by_significance_and_significance(self, submitter1_id = None, submitter2_id = None,
                                                                    min_stars1 = 0, min_stars2 = 0,
                                                                    standardized_method1 = None,
                                                                    standardized_method2 = None,
                                                                    original_terms = False):
        if original_terms:
            query = 'SELECT significance1, significance2'
        else:
            query = 'SELECT standardized_significance1 AS significance1, standardized_significance2 AS significance2'

        query += ', COUNT(DISTINCT variant_name) AS count FROM current_comparisons'

        query += ' WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=1'

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        if original_terms:
            query += ' GROUP BY significance1, significance2'
        else:
            query += ' GROUP BY standardized_significance1, standardized_significance2'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                }
            )
        ))

    def total_conflicting_variants_by_submitter_and_conflict_level(self, submitter1_id, min_stars1 = 0, min_stars2 = 0,
                                                                   standardized_method1 = None,
                                                                   standardized_method2 = None):
        query = '''
            SELECT
                submitter2_id AS submitter_id,
                submitter2_name AS submitter_name,
                conflict_level,
                COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE
                submitter1_id=:submitter1_id AND
                star_level1>=:min_stars1 AND
                star_level2>=:min_stars2 AND
                conflict_level>=1
        '''

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        query += ' GROUP BY submitter2_id, conflict_level ORDER BY submitter2_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                }
            )
        ))

    def total_significance_terms_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('SELECT date, COUNT(DISTINCT significance) AS count FROM submissions GROUP BY date')
        ))

    def total_submissions(self):
        return list(self.cursor.execute('SELECT COUNT(*) FROM current_submissions'))[0][0]

    def total_submissions_by_country(self, min_stars = 0, standardized_method = None, min_conflict_level = 0):
        query = '''
            SELECT
                submitter1_country_code AS country_code,
                submitter1_country_name AS country_name,
                COUNT(DISTINCT scv1) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method'

        query += ' GROUP BY country_code ORDER BY country_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_submissions_by_method(self, min_stars = 0, min_conflict_level = 0):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT method1 AS method, COUNT(DISTINCT scv1) AS count
                    FROM current_comparisons
                    WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
                    GROUP BY method ORDER BY method
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
        query = '''
            SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name, COUNT(DISTINCT scv1) AS count
            FROM current_comparisons
        '''

        query += ' WHERE star_level1>=:min_stars AND conflict_level>=:min_conflict_level'

        if country_code != None:
            query += ' AND submitter1_country_code=:country_code'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method'

        query += ' GROUP BY submitter1_id ORDER BY submitter1_name'

        try:
            return list(map(
                dict,
                self.cursor.execute(
                    query,
                    {
                        'country_code': country_code,
                        'min_stars': min_stars,
                        'standardized_method': standardized_method,
                        'min_conflict_level': min_conflict_level,
                    }
                )
            ))
        except OperationalError:
            return []

    def total_variants(self, submitter1_id = None, submitter2_id = None, min_stars1 = 0, min_stars2 = 0,
                       standardized_method1 = None, standardized_method2 = None, min_conflict_level = 0):
        query = '''
            SELECT COUNT(DISTINCT variant_name) FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        return list(
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                    'min_conflict_level': min_conflict_level,
                }
            )
        )[0][0]

    def total_variants_by_gene(self, min_stars = 0, standardized_method = None, min_conflict_level = 0):
        query = '''
            SELECT gene, COUNT(DISTINCT variant_name) AS count FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY gene ORDER BY gene'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_variants_by_gene_and_significance(self, trait_name = None, submitter_id = None, min_stars = 0,
                                                standardized_method = None, min_conflict_level = 0,
                                                original_terms = False):
        query = 'SELECT gene, COUNT(DISTINCT variant_name) AS count'

        if original_terms:
            query += ', significance1 AS significance'
        else:
            query += ', standardized_significance1 AS significance'

        query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if trait_name:
            query += ' AND trait1_name=:trait_name'

        if submitter_id:
            query += ' AND submitter1_id=:submitter_id'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY gene, significance ORDER BY gene, significance'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'trait_name': trait_name,
                    'submitter_id': submitter_id,
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def total_variants_by_significance(self, gene = None, trait_name = None, submitter_id = None, min_stars = 0,
                                       standardized_method = None, min_conflict_level = 0, original_terms = False):
        query = 'SELECT COUNT(DISTINCT variant_name) AS count'

        if original_terms:
            query += ', significance1 AS significance'
        else:
            query += ', standardized_significance1 AS significance'

        query += '''
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if gene:
            query += ' AND gene=:gene'

        if trait_name:
            query += ' AND trait1_name=:trait_name'

        if submitter_id:
            query += ' AND submitter1_id=:submitter_id'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY significance ORDER BY significance'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'trait_name': trait_name,
                    'submitter_id': submitter_id,
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def total_variants_by_submitter(self, submitter1_id = None, significance1 = None, min_stars1 = 0, min_stars2 = 0,
                                    standardized_method1 = None, standardized_method2 = None, min_conflict_level = 0):
        if submitter1_id:
            query = 'SELECT submitter2_id AS submitter_id, submitter2_name AS submitter_name'
        else:
            query = 'SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name'

        query += '''
            , COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if significance1:
            query += ' AND significance1=:significance1'

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        query += ' GROUP BY submitter_id ORDER BY submitter_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'significance1': significance1,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_variants_by_submitter_and_significance(self, gene = None, trait_name = None, min_stars = 0,
                                                     standardized_method = None, min_conflict_level = 0,
                                                     original_terms = False):
        query = '''
            SELECT
                submitter1_id AS submitter_id,
                submitter1_name AS submitter_name,
                COUNT(DISTINCT variant_name) AS count
        '''

        if original_terms:
            query += ', significance1 AS significance'
        else:
            query += ', standardized_significance1 AS significance'

        query += '''
            FROM current_comparisons
            WHERE
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level
        '''

        if gene != None:
            query += ' AND gene=:gene'

        if trait_name:
            query += ' AND trait1_name=:trait_name'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY submitter_id, significance ORDER BY submitter1_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'trait_name': trait_name,
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def total_variants_by_trait(self, min_stars = 0, standardized_method = None, min_conflict_level = 0):
        query = '''
            SELECT
                trait1_name AS trait_name,
                COUNT(DISTINCT variant_name) AS count
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY trait_name ORDER BY trait_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def total_variants_by_trait_and_significance(self, gene = None, submitter_id = None, min_stars = 0,
                                                 standardized_method = None, min_conflict_level = 0,
                                                 original_terms = False):
        query = '''
            SELECT
                trait1_db AS trait_db,
                trait1_id AS trait_id,
                trait1_name AS trait_name,
                COUNT(DISTINCT variant_name) AS count
        '''

        if original_terms:
            query += ', significance1 AS significance'
        else:
            query += ', standardized_significance1 AS significance'

        query += '''
            FROM current_comparisons
            WHERE
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level
        '''

        if gene != None:
            query += ' AND gene=:gene'

        if submitter_id:
            query += ' AND submitter1_id=:submitter_id'

        if standardized_method:
            query += ' AND standardized_method1=:standardized_method AND standardized_method2=:standardized_method'

        query += ' GROUP BY trait_name, significance ORDER BY trait_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'submitter_id': submitter_id,
                    'min_stars': min_stars,
                    'standardized_method': standardized_method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def trait_info(self, trait_name):
        try:
            row = list(self.cursor.execute('''
                SELECT trait_db, trait_id FROM current_submissions WHERE trait_name=? AND trait_id!='' LIMIT 1
            ''', [trait_name]))[0]
            return {'db': row[0], 'id': row[1], 'name': trait_name}
        except IndexError:
            return {'db': '', 'id': '', 'name': trait_name}

    def variant_info(self, variant_name):
        row = list(self.cursor.execute(
            'SELECT variant_id, variant_rsid FROM current_submissions WHERE variant_name=? LIMIT 1', [variant_name]
        ))[0]
        return {'id': row[0], 'name': variant_name, 'rsid': row[1]}

    def variant_name_from_rcv(self, rcv):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM current_submissions WHERE rcv=? LIMIT 1', [rcv]
            ))[0][0]
        except IndexError:
            return None

    def variant_name_from_rsid(self, rsid):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM current_submissions WHERE variant_rsid=? LIMIT 1', [rsid]
            ))[0][0]
        except IndexError:
            return None

    def variant_name_from_scv(self, scv):
        try:
            return list(self.cursor.execute(
                'SELECT variant_name FROM current_submissions WHERE scv=? LIMIT 1', [scv]
            ))[0][0]
        except IndexError:
            return None

    def variants(self, gene = None, trait1_name = None, submitter1_id = None, submitter2_id = None,
                 significance1 = None, significance2 = None, min_stars1 = 0, min_stars2 = 0,
                 standardized_method1 = None, standardized_method2 = None, min_conflict_level = 1,
                 original_terms = False):
        query = '''
            SELECT DISTINCT variant_name, variant_rsid FROM current_comparisons
            WHERE star_level1>=:min_stars1 AND star_level2>=:min_stars2 AND conflict_level>=:min_conflict_level
        '''

        if gene != None:
            query += ' AND gene=:gene'

        if trait1_name:
            query += ' AND trait1_name=:trait1_name'

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if significance1:
            if original_terms:
                query += ' AND significance1=:significance1'
            else:
                query += ' AND standardized_significance1=:significance1'

        if significance2:
            if original_terms:
                query += ' AND significance2=:significance2'
            else:
                query += ' AND standardized_significance2=:significance2'

        if standardized_method1:
            query += ' AND standardized_method1=:standardized_method1'

        if standardized_method2:
            query += ' AND standardized_method2=:standardized_method2'

        query += ' ORDER BY variant_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'trait1_name': trait1_name,
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'significance1': significance1,
                    'significance2': significance2,
                    'min_stars1': min_stars1,
                    'min_stars2': min_stars2,
                    'standardized_method1': standardized_method1,
                    'standardized_method2': standardized_method2,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))
