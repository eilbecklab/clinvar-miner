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
            ncbi_variation_id INTEGER,
            preferred_name TEXT,
            variant_type TEXT,
            gene_symbol TEXT,
            submitter_id INTEGER,
            submitter_name TEXT,
            rcv TEXT,
            scv TEXT,
            clin_sig TEXT,
            corrected_clin_sig TEXT,
            last_eval TEXT,
            review_status TEXT,
            star_level INTEGER,
            sub_condition TEXT,
            method TEXT,
            description TEXT,
            PRIMARY KEY (date, scv)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparisons (
            date TEXT,
            ncbi_variation_id TEXT,
            preferred_name TEXT,
            variant_type TEXT,
            gene_symbol TEXT,
            submitter1_id INTEGER,
            submitter1_name TEXT,
            rcv1 TEXT,
            scv1 TEXT,
            clin_sig1 TEXT,
            corrected_clin_sig1 TEXT,
            last_eval1 TEXT,
            review_status1 TEXT,
            star_level1 INTEGER,
            sub_condition1 TEXT,
            method1 TEXT,
            description1 TEXT,
            submitter2_id INTEGER,
            submitter2_name TEXT,
            rcv2 TEXT,
            scv2 TEXT,
            clin_sig2 TEXT,
            corrected_clin_sig2 TEXT,
            last_eval2 TEXT,
            review_status2 TEXT,
            star_level2 INTEGER,
            sub_condition2 TEXT,
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
    cursor.execute('CREATE INDEX IF NOT EXISTS ncbi_variation_id_index ON submissions (ncbi_variation_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_id_index ON submissions (submitter_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_name_index ON submissions (submitter_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig_index ON submissions (clin_sig)')
    cursor.execute('CREATE INDEX IF NOT EXISTS method_index ON submissions (method)')

    cursor.execute('CREATE INDEX IF NOT EXISTS date_index ON comparisons (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS ncbi_variation_id_index ON comparisons (ncbi_variation_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS preferred_name_index ON comparisons (preferred_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS gene_symbol_index ON comparisons (gene_symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter1_id_index ON comparisons (submitter1_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_id_index ON comparisons (submitter2_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter2_name_index ON comparisons (submitter2_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig1_index ON comparisons (clin_sig1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS clin_sig2_index ON comparisons (clin_sig2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS corrected_clin_sig1_index ON comparisons (corrected_clin_sig1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS corrected_clin_sig2_index ON comparisons (corrected_clin_sig2)')
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
        preferred_name_el = measure_set_el.find('./Name/ElementValue[@Type="Preferred"]')
        measure_el = measure_set_el.find('./Measure')
        gene_symbol_el = measure_el.find('./MeasureRelationship/Symbol/ElementValue[@Type="Preferred"]')

        for assertion_el in set_el.findall('./ClinVarAssertion'):
            scv_el = assertion_el.find('./ClinVarAccession[@Type="SCV"]')
            scv = scv_el.attrib['Acc']

            submission_id_el = assertion_el.find('./ClinVarSubmissionID')
            clin_sig_el = assertion_el.find('./ClinicalSignificance')
            description_el = clin_sig_el.find('./Description')
            review_status_el = clin_sig_el.find('./ReviewStatus')
            sub_condition_el = assertion_el.find('./TraitSet[@Type="PhenotypeInstruction"]/Trait[@Type="PhenotypeInstruction"]/Name/ElementValue[@Type="Preferred"]')
            method_el = assertion_el.find('./ObservedIn/Method/MethodType')
            comment_el = clin_sig_el.find('./Comment')

            ncbi_variation_id = int(measure_set_el.attrib['ID'])
            preferred_name = preferred_name_el.text if preferred_name_el != None else '' #missing in old versions
            variant_type = measure_el.attrib['Type']
            gene_symbol = gene_symbol_el.text if gene_symbol_el != None else ''
            submitter_id = int(scv_el.attrib['OrgID']) if scv_el.attrib.get('OrgID') else 0 #missing in old versions
            submitter_name = submission_id_el.get('submitter', '') if submission_id_el != None else '' #missing in old versions
            rcv = reference_assertion_el.find('./ClinVarAccession[@Type="RCV"]').attrib['Acc']
            clin_sig = description_el.text.lower() if description_el != None else 'not provided'
            corrected_clin_sig = nonstandard_significance_term_map.get(clin_sig, clin_sig)
            last_eval = clin_sig_el.attrib.get('DateLastEvaluated', '') #missing in old versions
            review_status = review_status_el.text if review_status_el != None else '' #missing in old versions
            sub_condition = sub_condition_el.text if sub_condition_el != None else ''
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
                ncbi_variation_id,
                preferred_name,
                variant_type,
                gene_symbol,
                submitter_id,
                submitter_name,
                rcv,
                scv,
                clin_sig,
                corrected_clin_sig,
                last_eval,
                review_status,
                star_level,
                sub_condition,
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
            t2.corrected_clin_sig,
            t2.last_eval,
            t2.review_status,
            t2.star_level,
            t2.sub_condition,
            t2.method,
            t2.description,
            CASE
                WHEN t1.corrected_clin_sig=t2.corrected_clin_sig AND t1.clin_sig!=t2.clin_sig THEN 1

                WHEN t1.corrected_clin_sig="benign" AND t2.corrected_clin_sig="likely benign" THEN 2
                WHEN t1.corrected_clin_sig="likely benign" AND t2.corrected_clin_sig="benign" THEN 2
                WHEN t1.corrected_clin_sig="pathogenic" AND t2.corrected_clin_sig="likely pathogenic" THEN 2
                WHEN t1.corrected_clin_sig="likely pathogenic" AND t2.corrected_clin_sig="pathogenic" THEN 2

                WHEN t1.corrected_clin_sig IN ("benign", "likely benign") AND t2.corrected_clin_sig="uncertain significance" THEN 3
                WHEN t1.corrected_clin_sig="uncertain significance" AND t2.corrected_clin_sig IN ("benign", "likely benign") THEN 3

                WHEN t1.corrected_clin_sig IN ("benign", "likely benign", "uncertain significance") AND t2.corrected_clin_sig IN ("pathogenic", "likely pathogenic") THEN 5
                WHEN t1.corrected_clin_sig IN ("pathogenic", "likely pathogenic") AND t2.corrected_clin_sig IN ("benign", "likely benign", "uncertain significance") THEN 5

                WHEN t1.corrected_clin_sig!=t2.corrected_clin_sig THEN 4

                ELSE 0
            END AS conflict_level
        FROM submissions t1 INNER JOIN submissions t2
        ON t1.date=? AND t1.date=t2.date AND t1.ncbi_variation_id=t2.ncbi_variation_id
    ''', [date])

    db.commit()
    db.close()

create_tables()
for filename in argv[1:]:
    import_file(filename)
