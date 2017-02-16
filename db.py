import sqlite3
from pycountry import countries
from sqlite3 import OperationalError

class DB():
    STAR_MAP = [
        'review_status2="criteria provided, single submitter" OR review_status2="criteria provided, conflicting interpretations"',
        'review_status2="criteria provided, multiple submitters, no conflicts"',
        'review_status2="reviewed by expert panel"',
        'review_status2="practice guideline"',
    ]

    def __init__(self):
        self.db = sqlite3.connect('clinvar.db', timeout=20)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def conflict_overview(self, submitter_id = None, min_stars = 0, method = None):
        query = '''
            SELECT corrected_clin_sig1, submitter2_id, submitter2_name, corrected_clin_sig2, COUNT(*) AS count
            FROM current_conflicts WHERE 1
        '''

        if submitter_id:
            query += ' AND submitter1_id=:submitter_id'

        if min_stars > 0:
            query += ' AND (' + ' OR '.join(DB.STAR_MAP[min_stars-1:]) + ')'

        if method:
            query += ' AND method2=:method'

        query += ' GROUP BY submitter2_id, corrected_clin_sig1, corrected_clin_sig2 ORDER BY submitter2_name'

        return list(map(
            dict,
            self.cursor.execute(
                query,
                {
                    'submitter_id': submitter_id,
                    'method': method
                }
            )
        ))

    def conflicting_submissions_by_gene(self, gene):
        return list(map(
            dict,
            self.cursor.execute('SELECT * FROM current_conflicting_submissions WHERE gene_symbol=?', [gene])
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
            query += ' AND (' + ' OR '.join(DB.STAR_MAP[min_stars-1:]) + ')'

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

    def corrected_significances(self):
        return list(map(
            lambda row: row[0],
            self.cursor.execute('''
                SELECT DISTINCT corrected_clin_sig FROM current_submissions ORDER BY corrected_clin_sig
            ''')
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

    def total_conflicting_submissions_by_gene(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT gene_symbol, COUNT(*) AS count FROM current_conflicts
                WHERE gene_symbol!="" GROUP BY gene_symbol ORDER BY gene_symbol
            ''')
        ))

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
