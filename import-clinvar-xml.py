#!/usr/bin/env python3

from collections import OrderedDict
from itertools import combinations
from os.path import basename
from pycountry import countries
from sys import argv
from xml.etree import ElementTree
import csv
import re
import sqlite3

if len(argv) < 2:
    print('Usage: ./import-clinvar-xml.py ClinVarFullRelease_<year>-<month>.xml ...')
    exit()

nonstandard_significance_term_map = dict(map(
    lambda line: line[0:-1].split('\t'),
    open('nonstandard_significance_terms.tsv')
))

submitter_country_codes = dict(map(
    lambda row: (int(row[0]), row[2]),
    csv.reader(open('submitter_info.tsv', 'r'), delimiter='\t')
))

standard_methods = [
    'clinical testing',
    'curation',
    'literature only',
    'research',
]

def connect():
    return sqlite3.connect('clinvar.db', timeout=600)

def create_tables():
    db = connect()
    cursor = db.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS submissions (
            date TEXT,
            variant_id INTEGER,
            variant_name TEXT,
            variant_rsid TEXT,
            gene TEXT,
            submitter_id INTEGER,
            submitter_name TEXT,
            submitter_country_code TEXT,
            submitter_country_name TEXT,
            rcv TEXT,
            scv TEXT,
            significance TEXT,
            standardized_significance TEXT,
            last_eval TEXT,
            review_status TEXT,
            star_level INTEGER,
            trait_db TEXT,
            trait_id TEXT,
            trait_name TEXT,
            method TEXT,
            standardized_method TEXT,
            comment TEXT,
            PRIMARY KEY (date, scv)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparisons (
            date TEXT,
            variant_id TEXT,
            variant_name TEXT,
            variant_rsid TEXT,
            gene TEXT,

            submitter1_id INTEGER,
            submitter1_name TEXT,
            submitter1_country_code TEXT,
            submitter1_country_name TEXT,
            rcv1 TEXT,
            scv1 TEXT,
            significance1 TEXT,
            standardized_significance1 TEXT,
            last_eval1 TEXT,
            review_status1 TEXT,
            star_level1 INTEGER,
            trait1_db TEXT,
            trait1_id TEXT,
            trait1_name TEXT,
            method1 TEXT,
            standardized_method1 TEXT,
            comment1 TEXT,

            submitter2_id INTEGER,
            submitter2_name TEXT,
            scv2 TEXT,
            significance2 TEXT,
            standardized_significance2 TEXT,
            star_level2 INTEGER,
            standardized_method2 TEXT,

            conflict_level INTEGER,

            PRIMARY KEY (date, scv1, scv2)
        )
    ''')

    cursor.execute('''
        CREATE VIEW IF NOT EXISTS current_submissions AS
        SELECT * FROM submissions WHERE date=(
            SELECT MAX(date) FROM submissions
        )
    ''')

    cursor.execute('''
        CREATE VIEW IF NOT EXISTS current_comparisons AS
        SELECT * FROM comparisons WHERE date=(
            SELECT MAX(date) FROM submissions
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS date_index ON submissions (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_name_index ON submissions (variant_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_rsid_index ON submissions (variant_rsid)')
    cursor.execute('CREATE INDEX IF NOT EXISTS gene_index ON submissions (gene)')
    cursor.execute('CREATE INDEX IF NOT EXISTS rcv_index ON submissions (rcv)')
    cursor.execute('CREATE INDEX IF NOT EXISTS scv_index ON submissions (scv)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_id_index ON submissions (submitter_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_name_index ON submissions (submitter_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_country_code_index ON submissions (submitter_country_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS significance_index ON submissions (significance)')
    cursor.execute('CREATE INDEX IF NOT EXISTS trait_id_index ON submissions (trait_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS trait_name_index ON submissions (trait_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method_index ON submissions (method)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_method_index ON submissions (standardized_method)')

    cursor.execute('CREATE INDEX IF NOT EXISTS date_index ON comparisons (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_name_index ON comparisons (variant_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS gene_index ON comparisons (gene)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_id_index ON comparisons (submitter1_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_name_index ON comparisons (submitter1_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_country_code_index ON comparisons (submitter1_country_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_country_name_index ON comparisons (submitter1_country_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS scv1_index ON comparisons(scv1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS significance1_index ON comparisons (significance1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_significance1_index ON comparisons (standardized_significance1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS star_level1_index ON comparisons (star_level1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS trait1_name_index ON comparisons (trait1_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method1_index ON comparisons (method1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_method1_index ON comparisons (standardized_method1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_id_index ON comparisons (submitter2_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_name_index ON comparisons (submitter2_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS significance2_index ON comparisons (significance2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_significance2_index ON comparisons (standardized_significance2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS star_level2_index ON comparisons (star_level2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_method2_index ON comparisons (standardized_method2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS conflict_level_index ON comparisons (conflict_level)')

def import_file(filename):
    matches = re.fullmatch(r'ClinVarFullRelease_(\d\d\d\d-\d\d).xml', basename(filename))
    if matches:
        print('Importing ' + filename)
    else:
        print('Skipped unrecognized filename ' + filename)
        return

    date = matches.group(1)
    submissions = []

    #extract submission information
    for event, set_el in ElementTree.iterparse(filename):
        if set_el.tag != 'ClinVarSet':
            continue

        reference_assertion_el = set_el.find('./ReferenceClinVarAssertion')
        rcv = reference_assertion_el.find('./ClinVarAccession[@Type="RCV"]').attrib['Acc']

        measure_set_el = reference_assertion_el.find('./MeasureSet')
        genotype_set_el = reference_assertion_el.find('./GenotypeSet')

        if genotype_set_el != None:
            variant_id = 0
            variant_name_el = genotype_set_el.find('./Name/ElementValue[@Type="Preferred"]')
            measure_els = genotype_set_el.findall('./MeasureSet/Measure')
        else:
            variant_id = int(measure_set_el.attrib['ID'])
            variant_name_el = measure_set_el.find('./Name/ElementValue[@Type="Preferred"]')
            measure_els = measure_set_el.findall('./Measure')

        variant_name = variant_name_el.text if variant_name_el != None else str(variant_id) #missing in old versions

        variant_rsid = ''
        if len(measure_els) == 1:
            rsid_el = measure_els[0].find('./XRef[@Type="rs"]')
            if rsid_el != None:
                variant_rsid = 'rs' + rsid_el.attrib['ID']

        genes = set()
        for measure_el in measure_els:
            gene_el = measure_el.find('./MeasureRelationship/Symbol/ElementValue[@Type="Preferred"]')
            if gene_el != None:
                genes.add(gene_el.text)
        if len(genes) == 1:
            gene = list(genes)[0]
        else:
            gene = ''

        trait_name_els = reference_assertion_el.findall('./TraitSet/Trait/Name/ElementValue[@Type="Preferred"]')
        if trait_name_els:
            trait_name = '; '.join(map(lambda el: el.text, trait_name_els))
        else:
            trait_name = 'not specified'

        for assertion_el in set_el.findall('./ClinVarAssertion'):
            scv_el = assertion_el.find('./ClinVarAccession[@Type="SCV"]')
            scv = scv_el.attrib['Acc']

            if len(trait_name_els) == 1:
                trait_xref_el = assertion_el.find('./TraitSet/Trait/XRef')
                trait_db = trait_xref_el.attrib['DB'] if trait_xref_el != None else ''
                trait_id = trait_xref_el.attrib['ID'] if trait_xref_el != None else ''
            else:
                trait_db = ''
                trait_id = ''

            submission_id_el = assertion_el.find('./ClinVarSubmissionID')
            significance_el = assertion_el.find('./ClinicalSignificance')
            description_el = significance_el.find('./Description')
            review_status_el = significance_el.find('./ReviewStatus')
            method_el = assertion_el.find('./ObservedIn/Method/MethodType')
            comment_el = significance_el.find('./Comment')

            submitter_id = int(scv_el.attrib['OrgID']) if scv_el.attrib.get('OrgID') else 0 #missing in old versions
            submitter_name = submission_id_el.get('submitter', '') if submission_id_el != None else '' #missing in old versions
            submitter_country_code = submitter_country_codes[submitter_id] if submitter_id in submitter_country_codes else ''
            if submitter_country_code:
                submitter_country = countries.get(alpha_3=submitter_country_code)
                if hasattr(submitter_country, 'common_name'):
                    submitter_country_name = submitter_country.common_name
                else:
                    submitter_country_name = submitter_country.name
            else:
                submitter_country_name = ''

            significance = description_el.text.lower() if description_el != None else 'not provided'
            standardized_significance = nonstandard_significance_term_map.get(significance, significance)
            last_eval = significance_el.attrib.get('DateLastEvaluated', '') #missing in old versions
            review_status = review_status_el.text if review_status_el != None else '' #missing in old versions
            method = method_el.text if method_el != None else 'not provided' #missing in old versions
            standardized_method = method if method in standard_methods else 'other'
            comment = comment_el.text if comment_el != None else ''

            if review_status in ['criteria provided, single submitter', 'criteria provided, conflicting interpretations']:
                star_level = 1
            elif review_status == 'reviewed by expert panel':
                star_level = 3
            elif review_status == 'practice guideline':
                star_level = 4
            else:
                star_level = 0

            submissions.append((
                date,
                variant_id,
                variant_name,
                variant_rsid,
                gene,
                submitter_id,
                submitter_name,
                submitter_country_code,
                submitter_country_name,
                rcv,
                scv,
                significance,
                standardized_significance,
                last_eval,
                review_status,
                star_level,
                trait_db,
                trait_id,
                trait_name,
                method,
                standardized_method,
                comment,
            ))

        set_el.clear() #conserve memory

    #do all the database imports at once to minimize the time that we hold the database lock

    db = connect()
    cursor = db.cursor()

    cursor.executemany(
        'INSERT OR REPLACE INTO submissions VALUES (' + ','.join('?' * len(submissions[0])) + ')', submissions
    )

    cursor.execute('''
        INSERT OR REPLACE INTO comparisons
        SELECT
            t1.*,
            t2.submitter_id,
            t2.submitter_name,
            t2.scv,
            t2.significance,
            t2.standardized_significance,
            t2.star_level,
            t2.standardized_method,
            CASE
                WHEN t1.standardized_significance=t2.standardized_significance AND t1.significance!=t2.significance THEN 1

                WHEN t1.standardized_significance="benign" AND t2.standardized_significance="likely benign" THEN 2
                WHEN t1.standardized_significance="likely benign" AND t2.standardized_significance="benign" THEN 2
                WHEN t1.standardized_significance="pathogenic" AND t2.standardized_significance="likely pathogenic" THEN 2
                WHEN t1.standardized_significance="likely pathogenic" AND t2.standardized_significance="pathogenic" THEN 2

                WHEN t1.standardized_significance IN ("benign", "likely benign") AND t2.standardized_significance="uncertain significance" THEN 3
                WHEN t1.standardized_significance="uncertain significance" AND t2.standardized_significance IN ("benign", "likely benign") THEN 3

                WHEN t1.standardized_significance IN ("benign", "likely benign", "uncertain significance") AND t2.standardized_significance IN ("pathogenic", "likely pathogenic") THEN 5
                WHEN t1.standardized_significance IN ("pathogenic", "likely pathogenic") AND t2.standardized_significance IN ("benign", "likely benign", "uncertain significance") THEN 5

                WHEN t1.standardized_significance!=t2.standardized_significance THEN 4

                ELSE 0
            END AS conflict_level
        FROM submissions t1 INNER JOIN submissions t2
        ON t1.date=? AND t1.date=t2.date AND t1.variant_name=t2.variant_name
    ''', [date])

    db.commit()
    db.close()

create_tables()
for filename in argv[1:]:
    import_file(filename)
