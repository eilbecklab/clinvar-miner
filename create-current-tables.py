#!/usr/bin/env python3

import sqlite3

db = __import__('import-clinvar-xml').connect()
cursor = db.cursor()

cursor.execute('DROP TABLE IF EXISTS current_submissions')

cursor.execute('''
    CREATE TABLE current_submissions AS
    SELECT * FROM submissions WHERE date=(
        SELECT MAX(date) FROM submissions
    )
''')

cursor.execute('CREATE INDEX IF NOT EXISTS variant_name_index ON current_submissions (variant_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS variant_rsid_index ON current_submissions (variant_rsid)')
cursor.execute('CREATE INDEX IF NOT EXISTS gene_index ON current_submissions (gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS rcv_index ON current_submissions (rcv)')
cursor.execute('CREATE INDEX IF NOT EXISTS scv_index ON current_submissions (scv)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter_id_index ON current_submissions (submitter_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter_name_index ON current_submissions (submitter_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter_country_code_index ON current_submissions (submitter_country_code)')
cursor.execute('CREATE INDEX IF NOT EXISTS significance_index ON current_submissions (significance)')
cursor.execute('CREATE INDEX IF NOT EXISTS condition_id_index ON current_submissions (condition_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS condition_name_index ON current_submissions (condition_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS method_index ON current_submissions (method)')

cursor.execute('DROP TABLE IF EXISTS current_comparisons')

cursor.execute('''
    CREATE TABLE current_comparisons AS
    SELECT * FROM comparisons WHERE date=(
        SELECT MAX(date) FROM comparisons
    )
''')

cursor.execute('CREATE INDEX IF NOT EXISTS variant_name_index ON current_comparisons (variant_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS gene_index ON current_comparisons (gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_id_index ON current_comparisons (submitter1_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_name_index ON current_comparisons (submitter1_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_country_code_index ON current_comparisons (submitter1_country_code)')
cursor.execute('CREATE INDEX IF NOT EXISTS scv1_index ON current_comparisons(scv1)')
cursor.execute('CREATE INDEX IF NOT EXISTS significance1_index ON current_comparisons (significance1)')
cursor.execute('CREATE INDEX IF NOT EXISTS standardized_significance1_index ON current_comparisons (standardized_significance1)')
cursor.execute('CREATE INDEX IF NOT EXISTS star_level1_index ON current_comparisons (star_level1)')
cursor.execute('CREATE INDEX IF NOT EXISTS condition1_name_index ON current_comparisons (condition1_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS method1_index ON current_comparisons (method1)')
cursor.execute('CREATE INDEX IF NOT EXISTS standardized_method1_index ON current_comparisons (standardized_method1)')
cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_id_index ON current_comparisons (submitter2_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS significance2_index ON current_comparisons (significance2)')
cursor.execute('CREATE INDEX IF NOT EXISTS standardized_significance2_index ON current_comparisons (standardized_significance2)')
cursor.execute('CREATE INDEX IF NOT EXISTS star_level2_index ON current_comparisons (star_level2)')
cursor.execute('CREATE INDEX IF NOT EXISTS standardized_method2_index ON current_comparisons (standardized_method2)')
cursor.execute('CREATE INDEX IF NOT EXISTS conflict_level_index ON current_comparisons (conflict_level)')

db.commit()
db.close()
