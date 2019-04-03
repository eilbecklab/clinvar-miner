#!/usr/bin/env python3

import sqlite3
from mondo import Mondo


print('Creating indexes')

db = __import__('import-clinvar-xml').connect()
cursor = db.cursor()

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


date = list(cursor.execute('SELECT MAX(date) FROM submissions'))[0][0]


print('Creating gene links table')

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

    query = 'SELECT DISTINCT ' + gene_column + ' FROM submissions WHERE ' + type_column + '=2 AND date=?'
    gene_combinations = list(map(lambda row: row[0], cursor.execute(query, [date])))

    query = 'SELECT 1 FROM submissions WHERE ' + gene_column + '=? AND date=?'
    for gene_combination in gene_combinations:
        for individual_gene in gene_combination.split(', '):
            is_gene = bool(list(cursor.execute(query, [individual_gene, date])))
            if is_gene:
                cursor.executemany(
                    'INSERT INTO ' + table + ' VALUES (?,?)',
                    [[gene_combination, individual_gene], [individual_gene, gene_combination]]
                )

    cursor.execute('CREATE INDEX ' + table + '__gene ON ' + table + ' (gene)')

create_gene_links_table(True)
create_gene_links_table(False)

print('Creating Mondo table')

mondo = Mondo()

cursor.execute('DROP TABLE IF EXISTS mondo_clinvar_relationships')
cursor.execute('''
    CREATE TABLE mondo_clinvar_relationships (
        mondo_id INTEGER,
        mondo_name TEXT,
        clinvar_name TEXT,
        PRIMARY KEY (mondo_name, clinvar_name)
    )
''')

for row in list(cursor.execute('SELECT DISTINCT condition_name, condition_xrefs FROM submissions WHERE date=?', [date])):
    clinvar_name = row[0]
    xrefs = row[1].split(';')
    for xref in xrefs:
        if xref.startswith('MONDO:'):
            mondo_name = mondo.mondo_xref_to_name[xref]
            mondo_id = xref[len('MONDO:'):]
            cursor.execute(
                'INSERT OR IGNORE INTO mondo_clinvar_relationships VALUES (?,?,?)',
                [mondo_id, mondo_name, clinvar_name]
            )

#add rows to associate ClinVar condition names with all of their Mondo ancestors
for row in list(cursor.execute('SELECT mondo_id, clinvar_name FROM mondo_clinvar_relationships')):
    mondo_id = row[0]
    clinvar_name = row[1]
    for ancestor_xref in mondo.ancestors('MONDO:' + str(mondo_id)):
        if ancestor_xref not in mondo.mondo_xref_to_name:
            continue #this is a deprecated term
        ancestor_id = ancestor_xref[len('MONDO:'):]
        ancestor_name = mondo.mondo_xref_to_name[ancestor_xref]
        cursor.execute(
            'INSERT OR IGNORE INTO mondo_clinvar_relationships VALUES (?,?,?)',
            [ancestor_id, ancestor_name, clinvar_name]
        )

cursor.execute('CREATE INDEX mondo_clinvar_relationships__mondo_id ON mondo_clinvar_relationships (mondo_id)')

db.commit()
db.close()
