#!/usr/bin/env python3

print('Creating indexes')

def create_index(cursor, table, columns):
    index = table + '__' + '__'.join(columns)
    columns = ','.join(columns)
    cursor.execute(f'CREATE INDEX IF NOT EXISTS {index} ON {table} ({columns})')

db = __import__('import-clinvar-xml').connect()
cursor = db.cursor()

submission_columns_to_index = [
    'rsid',
    'gene',
    'normalized_gene',
    'rcv',
    'submitter_id',
    'submitter_name',
    'submitter_country_code',
    'significance',
    'condition_name',
    'method',
]
create_index(cursor, 'submissions', ['date'])
for column in submission_columns_to_index:
    create_index(cursor, 'submissions', [column])
    create_index(cursor, 'submissions', ['date', column])

comparison_columns_to_index = [
    'variant_name',
    'gene',
    'gene_type',
    'normalized_gene',
    'normalized_gene_type',
    'submitter1_id',
    'submitter1_country_code',
    'scv1',
    'significance1',
    'normalized_significance1',
    'condition1_name',
    'method1',
    'normalized_method1',
    'submitter2_id',
    'significance2',
    'normalized_significance2',
    'normalized_method2',
    'condition2_name',
]
for column in comparison_columns_to_index:
    create_index(cursor, 'comparisons', ['date', 'conflict_level', 'star_level1', 'star_level2', column])

create_index(cursor, 'mondo_clinvar_relationships', ['mondo_id'])


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

db.commit()
db.close()
