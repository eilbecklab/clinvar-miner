import sqlite3
from pycountry import countries
from sqlite3 import OperationalError

class DB():
    STAR_MAP = [
        ['criteria provided, single submitter', 'criteria provided, conflicting interpretations'],
        ['criteria provided, multiple submitters, no conflicts'],
        ['reviewed by expert panel'],
        ['practice guideline'],
    ]

    def min_star_restriction(review_status_col, min_stars):
        return ' AND (' + ' OR '.join(map(
            lambda phrases: ' OR '.join(map(
                lambda phrase: review_status_col + '="' + phrase + '"',
                phrases
            )),
            DB.STAR_MAP[min_stars-1:]
        )) + ')'

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

        query += ', submitter2_id, submitter2_name, COUNT(*) AS count FROM current_conflicts WHERE 1'

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if min_stars > 0:
            query += DB.min_star_restriction('review_status2', min_stars)

        if method:
            query += ' AND method2=:method'

        if corrected_terms:
            query += ' GROUP BY corrected_clin_sig1, corrected_clin_sig2'
        else:
            query += ' GROUP BY clin_sig1, clin_sig2'

        query += ' ORDER BY submitter2_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter1_id': submitter1_id,
                    'submitter2_id': submitter2_id,
                    'method': method,
                }
            )
        ))

    def conflicts(self, submitter1_id = None, submitter2_id = None, significance1 = None, significance2 = None,
                  min_stars = 0, method = None):
        query = 'SELECT * FROM current_conflicts WHERE 1'

        if submitter1_id:
            query += ' AND submitter1_id=:submitter1_id'

        if submitter2_id:
            query += ' AND submitter2_id=:submitter2_id'

        if significance1:
            query += ' AND clin_sig1=:significance1'

        if significance2:
            query += ' AND clin_sig2=:significance2'

        if min_stars > 0:
            query += DB.min_star_restriction('review_status', min_stars)

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

    def submissions(self, conflicting = False, gene = None, variant_id = None, min_stars = 0, method = None):
        query = 'SELECT * FROM current_submissions WHERE 1'

        if conflicting:
            query += ' AND conflicting=1'

        if gene:
            query += ' AND gene_symbol=:gene'

        if variant_id:
            query += ' AND ncbi_variation_id=:variant_id'

        if min_stars > 0:
            query += DB.min_star_restriction('review_status', min_stars)

        if method:
            query += ' AND method=:method'

        query += ' ORDER BY submitter_name'

        return list(map(
            dict,
            self.cursor.execute(query, {'gene': gene, 'variant_id': variant_id, 'method': method})
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
                SELECT date, method, COUNT(*) AS count FROM conflicting_submissions
                GROUP BY date, method ORDER BY date, method
            ''')
        ))

    def total_conflicting_submissions_by_submitter(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT submitter_id, submitter_name, COUNT(*) AS count FROM current_conflicting_submissions
                GROUP BY submitter_id ORDER BY submitter_name
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
        ret = list(map(
            dict,
            self.cursor.execute('''
                SELECT country, COUNT(*) AS count FROM current_submissions
                LEFT JOIN submitter_info ON current_submissions.submitter_id=submitter_info.id
                GROUP BY country ORDER BY country
            ''')
        ))
        for row in ret:
            try:
                row['country_code'] = countries.lookup(row['country']).alpha_3
            except LookupError:
                pass
        return ret

    def total_submissions_by_gene(self, conflicting = False, submitter_id = None, min_stars = 0, method = None):
        query = 'SELECT gene_symbol, COUNT(*) AS count FROM current_submissions WHERE 1'

        if conflicting:
            query += ' AND conflicting=1'

        if submitter_id:
            query += ' AND submitter_id=:submitter_id'

        if min_stars > 0:
            query += DB.min_star_restriction('review_status', min_stars)

        if method:
            query += ' AND method=:method'

        query += ' GROUP BY gene_symbol ORDER BY gene_symbol'

        return list(map(
            dict,
            self.cursor.execute(query, {'submitter_id': submitter_id, 'method': method})
        ))

    def total_submissions_by_method_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT date, method, COUNT(*) AS count FROM submissions
                GROUP BY date, method ORDER BY date, method
            ''')
        ))

    def total_submissions_by_submitter(self, country):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT submitter_id, submitter_name, COUNT(*) AS count FROM current_submissions
                LEFT JOIN submitter_info ON current_submissions.submitter_id=submitter_info.id
                WHERE country=:country
                GROUP BY submitter_id ORDER BY submitter_name
            ''', [country])
        ))

    def total_submissions_by_variant(self, gene, conflicting = False, submitter_id = None, min_stars = 0, method = None):
        query = '''
            SELECT ncbi_variation_id, preferred_name, COUNT(*) AS count FROM current_submissions
            WHERE gene_symbol=:gene
        '''

        if conflicting:
            query += ' AND conflicting=1'

        if submitter_id:
            query += ' AND submitter_id=:submitter_id'

        if min_stars > 0:
            query += DB.min_star_restriction('review_status', min_stars)

        if method:
            query += ' AND method=:method'

        query += ' GROUP BY ncbi_variation_id ORDER BY preferred_name'

        return list(map(
            dict,
            self.cursor.execute(query, {'gene': gene, 'submitter_id': submitter_id, 'method': method})
        ))

    def variant_name(self, variant_id):
        return list(self.cursor.execute(
            'SELECT preferred_name FROM current_submissions WHERE ncbi_variation_id=?', [variant_id]
        ))[0][0]
