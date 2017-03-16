#!/usr/bin/env python3

from collections import OrderedDict
from itertools import combinations
from os.path import basename
from sys import argv
from xml.etree import ElementTree
import re
import sqlite3

if len(argv) < 2:
    print('Usage: ./import-clinvar-xml.py ClinVarFullRelease_<year>-<month>.xml ...')
    exit()

nonstandard_significance_term_map = dict(map(
    lambda line: line[0:-1].split('\t'),
    open('nonstandard_significance_terms.tsv')
))

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
            variant_type TEXT,
            gene TEXT,
            submitter_id INTEGER,
            submitter_name TEXT,
            rcv TEXT,
            scv TEXT,
            clin_sig TEXT,
            standardized_clin_sig TEXT,
            last_eval TEXT,
            review_status TEXT,
            star_level INTEGER,
            trait_db TEXT,
            trait_id TEXT,
            trait_name TEXT,
            method TEXT,
            description TEXT,
            PRIMARY KEY (date, scv)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparisons (
            date TEXT,
            variant_id TEXT,
            variant_name TEXT,
            variant_type TEXT,
            gene TEXT,
            submitter1_id INTEGER,
            submitter1_name TEXT,
            rcv1 TEXT,
            scv1 TEXT,
            clin_sig1 TEXT,
            standardized_clin_sig1 TEXT,
            last_eval1 TEXT,
            review_status1 TEXT,
            star_level1 INTEGER,
            trait1_db TEXT,
            trait1_id TEXT,
            trait1_name TEXT,
            method1 TEXT,
            description1 TEXT,
            submitter2_id INTEGER,
            submitter2_name TEXT,
            rcv2 TEXT,
            scv2 TEXT,
            clin_sig2 TEXT,
            standardized_clin_sig2 TEXT,
            last_eval2 TEXT,
            review_status2 TEXT,
            star_level2 INTEGER,
            trait2_db TEXT,
            trait2_id TEXT,
            trait2_name TEXT,
            method2 TEXT,
            description2 TEXT,
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
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_id_index ON submissions (variant_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_id_index ON submissions (submitter_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_name_index ON submissions (submitter_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig_index ON submissions (clin_sig)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method_index ON submissions (method)')

    cursor.execute('CREATE INDEX IF NOT EXISTS date_index ON comparisons (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_id_index ON comparisons (variant_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS variant_name_index ON comparisons (variant_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS gene_index ON comparisons (gene)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_id_index ON comparisons (submitter1_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_id_index ON comparisons (submitter2_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_name_index ON comparisons (submitter2_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig1_index ON comparisons (clin_sig1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig2_index ON comparisons (clin_sig2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_clin_sig1_index ON comparisons (standardized_clin_sig1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS standardized_clin_sig2_index ON comparisons (standardized_clin_sig2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS star_level1_index ON comparisons (star_level1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS star_level2_index ON comparisons (star_level2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method1_index ON comparisons (method1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method2_index ON comparisons (method2)')
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
        measure_set_el = reference_assertion_el.find('./MeasureSet')
        variant_name_el = measure_set_el.find('./Name/ElementValue[@Type="Preferred"]')
        measure_el = measure_set_el.find('./Measure')
        gene_el = measure_el.find('./MeasureRelationship/Symbol/ElementValue[@Type="Preferred"]')

        for assertion_el in set_el.findall('./ClinVarAssertion'):
            scv_el = assertion_el.find('./ClinVarAccession[@Type="SCV"]')
            scv = scv_el.attrib['Acc']

            submission_id_el = assertion_el.find('./ClinVarSubmissionID')
            clin_sig_el = assertion_el.find('./ClinicalSignificance')
            description_el = clin_sig_el.find('./Description')
            review_status_el = clin_sig_el.find('./ReviewStatus')
            trait_el = assertion_el.find('./TraitSet/Trait')
            trait_xref_el = trait_el.find('./XRef')
            trait_name_el = trait_el.find('./Name/ElementValue')
            method_el = assertion_el.find('./ObservedIn/Method/MethodType')
            comment_el = clin_sig_el.find('./Comment')

            variant_id = int(measure_set_el.attrib['ID'])
            variant_name = variant_name_el.text if variant_name_el != None else '' #missing in old versions
            variant_type = measure_el.attrib['Type']
            gene = gene_el.text if gene_el != None else ''
            submitter_id = int(scv_el.attrib['OrgID']) if scv_el.attrib.get('OrgID') else 0 #missing in old versions
            submitter_name = submission_id_el.get('submitter', '') if submission_id_el != None else '' #missing in old versions
            rcv = reference_assertion_el.find('./ClinVarAccession[@Type="RCV"]').attrib['Acc']
            clin_sig = description_el.text.lower() if description_el != None else 'not provided'
            standardized_clin_sig = nonstandard_significance_term_map.get(clin_sig, clin_sig)
            last_eval = clin_sig_el.attrib.get('DateLastEvaluated', '') #missing in old versions
            review_status = review_status_el.text if review_status_el != None else '' #missing in old versions
            trait_db = trait_xref_el.attrib['DB'] if trait_xref_el != None else ''
            trait_id = trait_xref_el.attrib['ID'] if trait_xref_el != None else ''
            trait_name = trait_name_el.text if trait_name_el != None else ''
            method = method_el.text if method_el != None else 'not provided' #missing in old versions
            description = comment_el.text if comment_el != None else ''

            if review_status in ['criteria provided, single submitter', 'criteria provided, conflicting interpretations']:
                star_level = 1
            elif review_status in ['criteria provided, multiple submitters, no conflicts']:
                star_level = 2
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
                variant_type,
                gene,
                submitter_id,
                submitter_name,
                rcv,
                scv,
                clin_sig,
                standardized_clin_sig,
                last_eval,
                review_status,
                star_level,
                trait_db,
                trait_id,
                trait_name,
                method,
                description,
            ))

        set_el.clear() #conserve memory

    #do all the database imports at once to minimize the time that we hold the database lock

    db = connect()
    cursor = db.cursor()

    cursor.executemany(
        'INSERT OR IGNORE INTO submissions VALUES (' + ','.join('?' * len(submissions[0])) + ')', submissions
    )

    cursor.execute('''
        INSERT OR IGNORE INTO comparisons
        SELECT
            t1.*,
            t2.submitter_id,
            t2.submitter_name,
            t2.rcv,
            t2.scv,
            t2.clin_sig,
            t2.standardized_clin_sig,
            t2.last_eval,
            t2.review_status,
            t2.star_level,
            t2.trait_db,
            t2.trait_id,
            t2.trait_name,
            t2.method,
            t2.description,
            CASE
                WHEN t1.standardized_clin_sig=t2.standardized_clin_sig AND t1.clin_sig!=t2.clin_sig THEN 1

                WHEN t1.standardized_clin_sig="benign" AND t2.standardized_clin_sig="likely benign" THEN 2
                WHEN t1.standardized_clin_sig="likely benign" AND t2.standardized_clin_sig="benign" THEN 2
                WHEN t1.standardized_clin_sig="pathogenic" AND t2.standardized_clin_sig="likely pathogenic" THEN 2
                WHEN t1.standardized_clin_sig="likely pathogenic" AND t2.standardized_clin_sig="pathogenic" THEN 2

                WHEN t1.standardized_clin_sig IN ("benign", "likely benign") AND t2.standardized_clin_sig="uncertain significance" THEN 3
                WHEN t1.standardized_clin_sig="uncertain significance" AND t2.standardized_clin_sig IN ("benign", "likely benign") THEN 3

                WHEN t1.standardized_clin_sig IN ("benign", "likely benign", "uncertain significance") AND t2.standardized_clin_sig IN ("pathogenic", "likely pathogenic") THEN 5
                WHEN t1.standardized_clin_sig IN ("pathogenic", "likely pathogenic") AND t2.standardized_clin_sig IN ("benign", "likely benign", "uncertain significance") THEN 5

                WHEN t1.standardized_clin_sig!=t2.standardized_clin_sig THEN 4

                ELSE 0
            END AS conflict_level
        FROM submissions t1 INNER JOIN submissions t2
        ON t1.date=? AND t1.date=t2.date AND t1.variant_id=t2.variant_id
    ''', [date])

    db.commit()
    db.close()

create_tables()
for filename in argv[1:]:
    import_file(filename)
