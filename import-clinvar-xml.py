#!/usr/bin/env python3

from itertools import combinations
from os.path import basename
from sys import argv
from xml.etree import ElementTree
import re
import sqlite3

if len(argv) < 2:
    print('Usage: ./import-clinvar-xml.py ClinVarFullRelease_<year>-<month>.xml ...')
    exit()

def connect():
    return sqlite3.connect('clinvar-conflicts.db', timeout=600)

def create_tables():
    db = connect()
    cursor = db.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS submission_counts (
            date TEXT,
            submitter_id TEXT,
            submitter_name TEXT,
            method TEXT,
            clin_sig TEXT,
            count INT,
            PRIMARY KEY (date, submitter_id, method, clin_sig)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conflicts (
            date TEXT,
            rcv TEXT,
            gene_symbol TEXT,
            ncbi_variation_id INT,
            preferred_name TEXT,
            variant_type TEXT,
            submitter_id TEXT,
            submitter_name TEXT,
            scv TEXT,
            clin_sig TEXT,
            last_eval TEXT,
            review_status TEXT,
            sub_condition TEXT,
            method TEXT,
            description TEXT,
            PRIMARY KEY (date, scv)
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS date_index ON conflicts (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS rcv_index ON conflicts (rcv)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submitter_id_index ON conflicts (submitter_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS scv_index ON conflicts (scv)')

    cursor.execute('''
        CREATE VIEW IF NOT EXISTS current_conflicts AS
        SELECT * FROM conflicts WHERE date=(
            SELECT MAX(date) FROM conflicts
        )
    ''')

    cursor.execute('''
        CREATE VIEW IF NOT EXISTS submitter_primary_method AS
        SELECT submitter_id, method FROM submission_counts t WHERE date=(
            SELECT MAX(date) max_date FROM submission_counts
        ) AND count=(
            SELECT MAX(count) FROM submission_counts WHERE submitter_id=t.submitter_id AND date=t.date
        )
    ''')

def import_file(filename):
    matches = re.fullmatch(r'ClinVarFullRelease_(\d\d\d\d-\d\d).xml', basename(filename))
    if matches:
        print('Importing ' + filename)
    else:
        print('Skipped unrecognized filename ' + filename)
        return

    date = matches.group(1)
    submission_counts = {}
    conflicts = []

    for event, set_el in ElementTree.iterparse(filename):
        if set_el.tag != 'ClinVarSet':
            continue

        assertion_els = list(set_el.findall('./ClinVarAssertion'))

        #find out how often each submitter uses each method
        for assertion_el in assertion_els:
            submission_id_el = assertion_el.find('./ClinVarSubmissionID')
            method_el = assertion_el.find('./ObservedIn/Method/MethodType')
            clin_sig_el = assertion_el.find('./ClinicalSignificance/Description')

            submitter_id = assertion_el.find('./ClinVarAccession[@Type="SCV"]').attrib.get('OrgID', '') #missing in old versions
            submitter_name = submission_id_el.attrib.get('submitter', '') if submission_id_el != None else '' #missing in old versions
            method = method_el.text if method_el != None else 'not provided' #missing in old versions
            clin_sig = clin_sig_el.text.lower() if clin_sig_el != None else ''

            if not submitter_id in submission_counts:
                submission_counts[submitter_id] = {'name': submitter_name, 'counts': {}}
            if not method in submission_counts[submitter_id]['counts']:
                submission_counts[submitter_id]['counts'][method] = {}
            if not clin_sig in submission_counts[submitter_id]['counts'][method]:
                submission_counts[submitter_id]['counts'][method][clin_sig] = 0
            submission_counts[submitter_id]['counts'][method][clin_sig] += 1

        #find conflicts
        conflicting_assertion_els = set()
        for assertion_el1, assertion_el2 in combinations(assertion_els, 2):
            clin_sig_el1 = assertion_el1.find('./ClinicalSignificance/Description')
            clin_sig_el2 = assertion_el2.find('./ClinicalSignificance/Description')

            clin_sig1 = clin_sig_el1.text.lower() if clin_sig_el1 != None else ''
            clin_sig2 = clin_sig_el2.text.lower() if clin_sig_el2 != None else ''

            if clin_sig1 != clin_sig2:
                conflicting_assertion_els.add(assertion_el1)
                conflicting_assertion_els.add(assertion_el2)

        for assertion_el in conflicting_assertion_els:
            reference_assertion_el = set_el.find('./ReferenceClinVarAssertion')
            measure_set_el = reference_assertion_el.find('./MeasureSet')
            measure_el = measure_set_el.find('./Measure')
            gene_symbol_el = measure_el.find('./MeasureRelationship/Symbol/ElementValue[@Type="Preferred"]')
            preferred_name_el = measure_set_el.find('./Name/ElementValue[@Type="Preferred"]')

            submission_id_el = assertion_el.find('./ClinVarSubmissionID')
            scv_el = assertion_el.find('./ClinVarAccession[@Type="SCV"]')
            clin_sig_el = assertion_el.find('./ClinicalSignificance')
            review_status_el = clin_sig_el.find('./ReviewStatus')
            sub_condition_el = assertion_el.find('./TraitSet[@Type="PhenotypeInstruction"]/Trait[@Type="PhenotypeInstruction"]/Name/ElementValue[@Type="Preferred"]')
            method_el = assertion_el.find('./ObservedIn/Method/MethodType')
            comment_el = clin_sig_el.find('./Comment')

            rcv = reference_assertion_el.find('./ClinVarAccession[@Type="RCV"]').attrib['Acc']
            gene_symbol = gene_symbol_el.text if gene_symbol_el != None else ''
            ncbi_variation_id = measure_set_el.attrib['ID']
            preferred_name = preferred_name_el.text if preferred_name_el != None else '' #missing in old versions
            variant_type = measure_el.attrib['Type']
            submitter_id = scv_el.attrib.get('OrgID', '') #missing in old versions
            submitter_name = submission_id_el.get('submitter', '') if submission_id_el != None else '' #missing in old versions
            scv = scv_el.attrib['Acc']
            clin_sig = clin_sig_el.find('./Description').text.lower()
            last_eval = clin_sig_el.attrib.get('DateLastEvaluated', '') #missing in old versions
            review_status = review_status_el.text if review_status_el != None else '' #missing in old versions
            sub_condition = sub_condition_el.text if sub_condition_el != None else ''
            method = method_el.text if method_el != None else 'not provided' #missing in old versions
            description = comment_el.text if comment_el != None else ''

            conflicts.append((
                date,
                rcv,
                gene_symbol,
                ncbi_variation_id,
                preferred_name,
                variant_type,
                submitter_id,
                submitter_name,
                scv,
                clin_sig,
                last_eval,
                review_status,
                sub_condition,
                method,
                description,
            ))

        set_el.clear() #conserve memory

    #do all the database imports at once to minimize the time that we hold the database lock

    db = connect()
    cursor = db.cursor()

    for submitter_id in submission_counts:
        submitter_name = submission_counts[submitter_id]['name']
        for method in submission_counts[submitter_id]['counts']:
            for clin_sig in submission_counts[submitter_id]['counts'][method]:
                count = submission_counts[submitter_id]['counts'][method][clin_sig]
                cursor.execute(
                    'INSERT OR IGNORE INTO submission_counts VALUES (?,?,?,?,?,?)',
                    (date, submitter_id, submitter_name, method, clin_sig, count)
                )

    cursor.executemany('INSERT OR IGNORE INTO conflicts VALUES (' + ','.join('?' * len(conflicts[0])) + ')', conflicts)

    db.commit()
    db.close()

create_tables()
for filename in argv[1:]:
    import_file(filename)
