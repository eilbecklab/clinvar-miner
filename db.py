import sqlite3
from sqlite3 import OperationalError

class DB():
    def __init__(self):
        self.db = sqlite3.connect('clinvar.db', timeout=20)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def conflict_overview(self, submitter1_id = None, submitter2_id = None, min_stars = 0, method = None,
                          corrected_terms = False):
        if corrected_terms:
            query = 'SELECT corrected_clin_sig1 AS clin_sig1, corrected_clin_sig2 AS clin_sig2'
        else:
            query = 'SELECT clin_sig1, clin_sig2'

        query += ', submitter2_id, submitter2_name, conflict_level, COUNT(DISTINCT ncbi_variation_id) AS count'
        query += ' FROM current_comparisons'

        query += ' WHERE star_level2>=:min_stars AND conflict_level>=1'

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if method:
            query += ' AND method2=:method'

        query += ' GROUP BY clin_sig1, clin_sig2, conflict_level, submitter2_id ORDER BY submitter2_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'min_stars': min_stars,
                    'method': method,
                }
            )
        ))

    def max_date(self):
        return list(self.cursor.execute('SELECT MAX(date) FROM submissions'))[0][0]

    def methods(self):
        return list(map(
            lambda row: row[0],
            self.cursor.execute('SELECT DISTINCT method FROM current_submissions ORDER BY method')
        ))

    def old_significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT * FROM (
                    SELECT clin_sig, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submissions
                    GROUP BY clin_sig ORDER BY first_seen DESC
                ) WHERE last_seen!=(SELECT MAX(date) FROM submissions)
            ''')
        ))

    def significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT clin_sig, MIN(date) AS first_seen FROM current_submissions
                GROUP BY clin_sig ORDER BY first_seen DESC
            ''')
        ))

    def submissions(self, gene = None, variant_id = None, min_stars = 0, method = None, min_conflict_level = 0):
        query = '''
            SELECT DISTINCT
                ncbi_variation_id,
                preferred_name,
                variant_type,
                gene_symbol,
                submitter1_id AS submitter_id,
                submitter1_name AS submitter_name,
                rcv1 AS rcv,
                scv1 AS scv,
                clin_sig1 AS clin_sig,
                corrected_clin_sig1 AS corrected_clin_sig1,
                last_eval1 AS last_eval,
                review_status1 AS review_status,
                sub_condition1 AS sub_condition,
                method1 AS method,
                description1 AS description
            FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if gene:
            query += ' AND gene_symbol=:gene'

        if variant_id:
            query += ' AND ncbi_variation_id=:variant_id'

        if method:
            query += ' AND method1=:method AND method2=:method'

        query += ' ORDER BY submitter_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'variant_id': variant_id,
                    'min_stars': min_stars,
                    'method': method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def submitter_info(self, submitter_id):
        try:
            return dict(list(self.cursor.execute('SELECT * from submitter_info WHERE id=:id', [submitter_id]))[0])
        except (IndexError, OperationalError):
            return None

    def submitter_primary_method(self, submitter_id):
        return list(
            self.cursor.execute('''
                SELECT method FROM current_submissions WHERE submitter_id=?
                GROUP BY method ORDER BY COUNT(*) DESC LIMIT 1
            ''', [submitter_id])
        )[0][0]

    def total_conflicting_submissions_by_method_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT date, method1 AS method, COUNT(DISTINCT scv1) AS count FROM comparisons
                WHERE conflict_level>=1
                GROUP BY date, method ORDER BY date, method
            ''')
        ))

    def total_significance_terms(self, term):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT submitter_id, submitter_name, COUNT(*) AS count FROM current_submissions WHERE clin_sig=?
                GROUP BY submitter_id ORDER BY submitter_name
            ''', [term])
        ))

    def total_significance_terms_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('SELECT date, COUNT(DISTINCT clin_sig) AS count FROM submissions GROUP BY date')
        ))

    def total_submissions_by_country(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT country, country_code, COUNT(*) AS count FROM current_submissions
                LEFT JOIN submitter_info ON current_submissions.submitter_id=submitter_info.id
                GROUP BY country ORDER BY country
            ''')
        ))

    def total_submissions_by_gene(self, submitter_id = None, min_stars = 0, method = None, min_conflict_level = 0):
        query = '''
            SELECT gene_symbol, COUNT(DISTINCT scv1) AS count FROM current_comparisons
            WHERE star_level1>=:min_stars AND star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if submitter_id:
            query += ' AND submitter_id=:submitter_id'

        if method:
            query += ' AND method1=:method AND method2=:method'

        query += ' GROUP BY gene_symbol ORDER BY gene_symbol'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter_id': submitter_id,
                    'min_stars': min_stars,
                    'method': method,
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def total_submissions_by_method_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT date, method, COUNT(*) AS count FROM submissions
                GROUP BY date, method ORDER BY date, method
            ''')
        ))

    def total_submissions_by_submitter(self, country = None, min_conflict_level = 0):
        query = '''
            SELECT submitter1_id AS submitter_id, submitter1_name AS submitter_name, COUNT(DISTINCT scv1) AS count
            FROM current_comparisons
        '''

        if country:
            query += ' LEFT JOIN submitter_info ON current_comparisons.submitter1_id=submitter_info.id'

        query += ' WHERE conflict_level>=:min_conflict_level'

        if country:
            query += ' AND country=:country'

        query += ' GROUP BY submitter1_id ORDER BY submitter1_name'

        return list(map(
            dict,
            self.cursor.execute(query, {'country': country, 'min_conflict_level': min_conflict_level})
        ))

    def total_submissions_by_variant(self, gene, min_stars = 0, method = None, min_conflict_level = 0):
        query = '''
            SELECT ncbi_variation_id, preferred_name, COUNT(DISTINCT scv1) AS count FROM current_comparisons
            WHERE
                gene_symbol=:gene AND
                star_level1>=:min_stars AND
                star_level2>=:min_stars AND
                conflict_level>=:min_conflict_level
        '''

        if method:
            query += ' AND method1=:method AND method2=:method'

        query += ' GROUP BY ncbi_variation_id ORDER BY preferred_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'gene': gene,
                    'min_stars': min_stars,
                    'method': method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))

    def total_variants_by_submitter(self, min_conflict_level = 0):
        return list(map(
            dict,
            self.cursor.execute(
                '''
                    SELECT
                        submitter1_id AS submitter_id,
                        submitter1_name AS submitter_name,
                        COUNT(DISTINCT ncbi_variation_id) AS count
                    FROM current_comparisons
                    WHERE conflict_level>=:min_conflict_level
                    GROUP BY submitter1_id ORDER BY submitter1_name
                ''',
                {
                    'min_conflict_level': min_conflict_level
                }
            )
        ))

    def variant_name(self, variant_id):
        return list(self.cursor.execute(
            'SELECT preferred_name FROM current_submissions WHERE ncbi_variation_id=?', [variant_id]
        ))[0][0]

    def variants(self, submitter1_id = None, submitter2_id = None, significance1 = None, significance2 = None,
                 min_stars = 0, method = None, min_conflict_level = 1, corrected_terms = False):
        query = '''
            SELECT DISTINCT ncbi_variation_id, preferred_name FROM current_comparisons
            WHERE star_level2>=:min_stars AND conflict_level>=:min_conflict_level
        '''

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if corrected_terms:
            if significance1:
                query += ' AND corrected_clin_sig1=:significance1'
            if significance2:
                query += ' AND corrected_clin_sig2=:significance2'
        else:
            if significance1:
                query += ' AND clin_sig1=:significance1'
            if significance2:
                query += ' AND clin_sig2=:significance2'

        if method:
            query += ' AND method2=:method'

        query += ' ORDER BY preferred_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'significance1': significance1,
                    'significance2': significance2,
                    'min_stars': min_stars,
                    'method': method,
                    'min_conflict_level': min_conflict_level,
                }
            )
        ))
