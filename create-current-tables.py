#!/usr/bin/env python3

import sqlite3

print('Creating current tables')

db = __import__('import-clinvar-xml').connect()
cursor = db.cursor()

cursor.execute('''
    CREATE VIEW IF NOT EXISTS current_submissions AS
    SELECT * FROM submissions WHERE date=(SELECT MAX(date) FROM submissions)
''')

cursor.execute('CREATE INDEX IF NOT EXISTS submissions__date ON submissions (date)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__variant_name ON submissions (variant_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__rsid ON submissions (rsid)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__gene ON submissions (gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__normalized_gene ON submissions (normalized_gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__rcv ON submissions (rcv)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__scv ON submissions (scv)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__submitter_id ON submissions (submitter_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__submitter_name ON submissions (submitter_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__submitter_country_code ON submissions (submitter_country_code)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__significance ON submissions (significance)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__condition_name ON submissions (condition_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__condition_xrefs ON submissions (condition_xrefs)')
cursor.execute('CREATE INDEX IF NOT EXISTS submissions__method ON submissions (method)')

cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__date ON comparisons (date)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__variant_name ON comparisons (variant_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__gene ON comparisons (gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__gene_type ON comparisons (gene_type)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_gene ON comparisons (normalized_gene)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_gene_type ON comparisons (normalized_gene_type)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__submitter1_id ON comparisons (submitter1_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__submitter1_name ON comparisons (submitter1_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__submitter1_country_code ON comparisons (submitter1_country_code)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__scv1 ON comparisons(scv1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__significance1 ON comparisons (significance1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_significance1 ON comparisons (normalized_significance1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__star_level1 ON comparisons (star_level1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__condition1_name ON comparisons (condition1_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__method1 ON comparisons (method1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_method1 ON comparisons (normalized_method1)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__submitter2_id ON comparisons (submitter2_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__significance2 ON comparisons (significance2)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_significance2 ON comparisons (normalized_significance2)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__star_level2 ON comparisons (star_level2)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_method2 ON comparisons (normalized_method2)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__condition2_name ON comparisons (condition2_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__conflict_level ON comparisons (conflict_level)')

def create_gene_links_table(normalized):
    if normalized:
        table = 'normalized_gene_links'
        gene_column = 'normalized_gene'
        type_column = 'normalized_gene_type'
    else:
        table = 'gene_links'
        gene_column = 'gene'
        type_column = 'gene_type'

    cursor.execute('DROP TABLE IF EXISTS ' + table)

    cursor.execute('CREATE TABLE ' + table + ' (gene TEXT, see_also TEXT)')

    gene_combinations = list(map(
        lambda row: row[0],
        cursor.execute('SELECT DISTINCT ' + gene_column + ' FROM current_submissions WHERE ' + type_column + '=2')
    ))
    for gene_combination in gene_combinations:
        for individual_gene in gene_combination.split(', '):
            is_gene = bool(list(cursor.execute(
                'SELECT 1 FROM current_submissions WHERE ' + gene_column + '=? LIMIT 1', [individual_gene]
            )))
            if is_gene:
                cursor.executemany(
                    'INSERT INTO ' + table + ' VALUES (?,?)',
                    [[gene_combination, individual_gene], [individual_gene, gene_combination]]
                )

    cursor.execute('CREATE INDEX ' + table + '__gene ON ' + table + ' (gene)')

create_gene_links_table(True)
create_gene_links_table(False)

db.commit()
db.close()
