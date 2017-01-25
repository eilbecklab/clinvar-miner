import sqlite3
from sqlite3 import OperationalError

class DB():
    STAR_MAP = [
        'c2.review_status="criteria provided, multiple submitter_ids, no conflicts" OR c2.review_status="criteria provided, single submitter_id"',
        'c2.review_status="criteria provided, multiple submitter_ids, no conflicts"',
        'c2.review_status="reviewed by expert panel"',
        'c2.review_status="practice guideline"',
    ]

    def __init__(self):
        self.db = sqlite3.connect('clinvar-conflicts.db', timeout=20)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()

    def conflict_overview(self, submitter_id = None, min_stars = 0, method = None):
        query = '''
            SELECT c1.clin_sig AS clin_sig1, c2.submitter_id AS submitter2_id, c2.submitter_name AS submitter2_name,
            c2.clin_sig AS clin_sig2, COUNT(c2.clin_sig) AS count
            FROM current_conflicts c1 INNER JOIN current_conflicts c2 ON c1.rcv=c2.rcv
            WHERE c1.clin_sig!=c2.clin_sig
        '''

        if submitter_id:
            query += ' AND c1.submitter_id=:submitter_id'

        if min_stars > 0:
            query += ' AND (' + ' OR '.join(DB.STAR_MAP[min_stars-1:]) + ')'

        if method:
            query += ' AND c2.method=:method'

        query += ' GROUP BY c2.submitter_id, c2.clin_sig'

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

    def conflicts_by_gene(self, gene = None):
        return list(map(
            dict,
            self.cursor.execute('SELECT * FROM current_conflicts WHERE gene_symbol=? ORDER BY preferred_name', [gene])
        ))

    def conflicts_by_submitter(self, submitter1_id = None, submitter2_id = None, significance1 = None,
                               significance2 = None, min_stars = 0, method = None):
        query = '''
            SELECT c1.rcv AS rcv, c1.gene_symbol AS gene_symbol, c1.ncbi_variation_id AS ncbi_variation_id,
            c1.preferred_name AS preferred_name, c1.variant_type AS variant_type, c1.scv AS scv1,
            c1.clin_sig AS clin_sig1, c1.last_eval AS last_eval1, c1.review_status AS review_status1,
            c1.sub_condition AS sub_condition1, c2.method AS method1, c1.description AS description1,
            c2.submitter_id AS submitter2_id, c2.submitter_name AS submitter2_name, c2.scv AS scv2,
            c2.clin_sig AS clin_sig2, c2.last_eval AS last_eval2, c2.review_status AS review_status2,
            c2.sub_condition AS sub_condition2, c2.method AS method2, c2.description AS description2
            FROM current_conflicts c1 INNER JOIN current_conflicts c2 ON c1.rcv=c2.rcv
            WHERE c1.clin_sig!=c2.clin_sig
        '''

        if submitter1_id:
            query += ' AND c1.submitter_id=:submitter1_id'

        if submitter2_id:
            query += ' AND c2.submitter_id=:submitter2_id'

        if significance1:
            query += ' AND c1.clin_sig=:significance1'

        if significance2:
            query += ' AND c2.clin_sig=:significance2'

        if min_stars > 0:
            query += ' AND (' + ' OR '.join(DB.STAR_MAP[min_stars-1:]) + ')'

        if method:
            query += ' AND c2.method=:method'

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
        return list(self.cursor.execute('SELECT MAX(date) FROM submission_counts'))[0][0]

    def methods(self):
        return list(map(
            lambda row: row[0],
            self.cursor.execute('SELECT DISTINCT method FROM current_conflicts ORDER BY method')
        ))

    def old_significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT * FROM (
                    SELECT clin_sig, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submission_counts
                    GROUP BY clin_sig ORDER BY first_seen DESC
                ) WHERE last_seen!=(SELECT MAX(date) FROM submission_counts)
            ''')
        ))

    def significance_term_info(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT clin_sig, first_seen FROM (
                    SELECT clin_sig, MIN(date) AS first_seen, MAX(date) AS last_seen FROM submission_counts
                    GROUP BY clin_sig ORDER BY first_seen DESC
                ) WHERE last_seen=(SELECT MAX(date) FROM submission_counts)
            ''')
        ))

    def significances(self):
        return list(map(
            lambda row: row[0],
            self.cursor.execute('SELECT DISTINCT clin_sig FROM current_conflicts ORDER BY clin_sig')
        ))

    def submitter_info(self, submitter_id):
        try:
            return dict(list(self.cursor.execute('SELECT * from submitter_info WHERE id=:id', [submitter_id]))[0])
        except (IndexError, OperationalError):
            return None

    def submitter_primary_method(self, submitter_id):
        return list(
            self.cursor.execute('SELECT method FROM submitter_primary_method WHERE submitter_id=:submitter_id', [submitter_id])
        )[0][0]

    def total_conflicts_by_gene(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT gene_symbol, COUNT(*) AS count FROM current_conflicts
                WHERE gene_symbol!="" GROUP BY gene_symbol ORDER BY gene_symbol
            ''')
        ))

    def total_conflicts_by_method_over_time(self):
        return list(map(
            dict,
            self.cursor.execute(
                'SELECT date, method, COUNT(method) AS count FROM conflicts GROUP BY date, method ORDER BY date, method'
            )
        ))

    def total_conflicts_by_submitter(self):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT submitter_id, submitter_name, COUNT(*) AS count
                FROM current_conflicts
                GROUP BY submitter_id
                ORDER BY submitter_name
            ''')
        ))

    def total_significance_terms(self, term):
        return list(map(
            dict,
            self.cursor.execute('''
                SELECT submitter_id, submitter_name, SUM(count) AS count FROM submission_counts
                WHERE date=(SELECT MAX(date) FROM submission_counts) AND clin_sig=?
                GROUP BY submitter_id ORDER BY submitter_name
            ''', [term])
        ))

    def total_significance_terms_over_time(self):
        return list(map(
            dict,
            self.cursor.execute('SELECT date, COUNT(DISTINCT clin_sig) AS count FROM submission_counts GROUP BY date')
        ))

    def total_submissions_by_method_over_time(self):
        return list(map(
            dict,
            self.cursor.execute(
                'SELECT date, method, SUM(count) AS count FROM submission_counts GROUP BY date, method ORDER BY date, method'
            )
        ))
