#!/usr/bin/env python3

from collections import OrderedDict
from copy import copy
from functools import partial
from mmap import mmap
from mondo import Mondo
from multiprocessing import Pool
from os.path import basename
from pycountry import countries
from sys import argv
from xml.etree import ElementTree
import csv
import re
import sqlite3

nonstandard_significance_term_map = dict(map(
    lambda line: line[0:-1].split('\t'),
    open('nonstandard_significance_terms.tsv')
))

submitter_country_codes = dict(map(
    lambda row: (int(row[0]), row[2]),
    csv.reader(open('submitter_info.tsv', 'r'), delimiter='\t')
))

mondo = Mondo()

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
            rsid TEXT,
            gene TEXT,
            gene_type INTEGER,
            normalized_gene TEXT,
            normalized_gene_type INTEGER,
            submitter_id INTEGER,
            submitter_name TEXT,
            submitter_country_code TEXT,
            submitter_country_name TEXT,
            rcv TEXT,
            scv TEXT,
            significance TEXT,
            normalized_significance TEXT,
            last_eval TEXT,
            review_status TEXT,
            star_level INTEGER,
            condition_name TEXT,
            condition_xrefs TEXT,
            method TEXT,
            normalized_method TEXT,
            comment TEXT,
            PRIMARY KEY (date, scv)
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS submissions__date ON submissions (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submissions__variant_name ON submissions (variant_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS submissions__significance ON submissions (significance)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparisons (
            date TEXT,
            variant_id TEXT,
            variant_name TEXT,
            rsid TEXT,
            gene TEXT,
            gene_type INTEGER,
            normalized_gene TEXT,
            normalized_gene_type INTEGER,

            submitter1_id INTEGER,
            submitter1_name TEXT,
            submitter1_country_code TEXT,
            submitter1_country_name TEXT,
            rcv1 TEXT,
            scv1 TEXT,
            significance1 TEXT,
            normalized_significance1 TEXT,
            last_eval1 TEXT,
            review_status1 TEXT,
            star_level1 INTEGER,
            condition1_name TEXT,
            condition1_xrefs TEXT,
            method1 TEXT,
            normalized_method1 TEXT,
            comment1 TEXT,

            submitter2_id INTEGER,
            submitter2_name TEXT,
            scv2 TEXT,
            significance2 TEXT,
            normalized_significance2 TEXT,
            star_level2 INTEGER,
            condition2_name TEXT,
            normalized_method2 TEXT,

            conflict_level INTEGER,

            PRIMARY KEY (date, scv1, scv2)
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__date ON comparisons (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__scv1 ON comparisons (scv1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__star_level1 ON comparisons (star_level1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__normalized_method1 ON comparisons (normalized_method1)')
    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__star_level2 ON comparisons (star_level2)')
    cursor.execute('CREATE INDEX IF NOT EXISTS comparisons__conflict_level ON comparisons (conflict_level)')

def get_gene_type(genes, small_variant):
    if len(genes) == 0:
        return 0 #intergenic
    elif len(genes) == 1:
        return 1 #in or near a single gene
    elif small_variant:
        return 2 #multiple genes because genes are close or overlap
    else:
        return 3 #multiple genes because variant is large

def get_submissions(date, set_xml):
    set_el = ElementTree.fromstring(set_xml)
    submissions = []

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

    rsid = ''
    if len(measure_els) == 1:
        rsid_el = measure_els[0].find('./XRef[@Type="rs"]')
        if rsid_el != None:
            rsid = 'rs' + rsid_el.attrib['ID']

    genes = set()
    small_variant = True
    first_variant_genes = None

    #loop through each individual variant in the compound variant
    for i, measure_el in enumerate(measure_els):
        #loop through each gene associated with the variant
        variant_genes = set()
        for relationship_el in measure_el.findall('./MeasureRelationship'):
            if relationship_el.attrib['Type'] == 'genes overlapped by variant':
                small_variant = False #probably a large deletion

            gene_el = relationship_el.find('./Symbol/ElementValue[@Type="Preferred"]')
            if gene_el != None and gene_el.text: #blank in old versions
                variant_genes.add(gene_el.text)

        #if the compound variant is small, each individual variant should be annotated with the same genes
        if i == 0:
            first_variant_genes = copy(variant_genes)
        elif variant_genes != first_variant_genes:
            small_variant = False

        genes |= variant_genes

    gene = ', '.join(sorted(genes))
    gene_type = get_gene_type(genes, small_variant)

    genes = set(map(lambda gene: gene.rpartition('-')[0] if re.fullmatch('.+-AS[1-9]?', gene) else gene, genes))
    normalized_gene = ', '.join(sorted(genes))
    normalized_gene_type = get_gene_type(genes, small_variant)

    trait_name_els = reference_assertion_el.findall('./TraitSet/Trait/Name/ElementValue[@Type="Preferred"]')
    if trait_name_els:
        condition_name = '; '.join(map(lambda el: el.text, trait_name_els))
    else:
        condition_name = 'not specified'

    condition_xrefs = set()
    for trait_xref_el in reference_assertion_el.findall('./TraitSet/Trait//XRef'):
        if trait_xref_el.attrib.get('Type') == 'secondary':
            continue
        condition_db = trait_xref_el.attrib['DB'].lower()
        condition_id = trait_xref_el.attrib['ID']
        #check for the most popular databases first
        if condition_db == 'medgen':
            condition_xrefs.add('UMLS:' + condition_id)
        elif condition_db == 'omim':
            condition_xrefs.add('OMIM:' + condition_id)
        elif condition_db == 'orphanet':
            condition_xrefs.add('ORPHANET:' + condition_id)
        elif condition_db == 'human phenotype ontology':
            condition_xrefs.add(condition_id) #already starts with 'HP:'
        elif condition_db == 'snomed ct':
            condition_xrefs.add('SNOMEDCT_US:' + condition_id)
        elif condition_db == 'mesh':
            condition_xrefs.add('MESH:' + condition_id)
        elif condition_db == 'uniprotkb/swiss-prot':
            condition_xrefs.add('UNIPROT:' + condition_id)
        elif condition_db == 'efo':
            condition_xrefs.add('EFO:' + condition_id)
    condition_xrefs |= set(mondo.exact_matches(condition_name, condition_xrefs))
    condition_xrefs = ';'.join(sorted(condition_xrefs))

    for assertion_el in set_el.findall('./ClinVarAssertion'):
        scv_el = assertion_el.find('./ClinVarAccession[@Type="SCV"]')
        scv = scv_el.attrib['Acc']

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
        normalized_significance = nonstandard_significance_term_map.get(significance, significance)
        last_eval = significance_el.attrib.get('DateLastEvaluated', '') #missing in old versions
        review_status = review_status_el.text if review_status_el != None else '' #missing in old versions
        method = method_el.text if method_el != None else 'not provided' #missing in old versions
        normalized_method = method if method in standard_methods else 'other'
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
            rsid,
            gene,
            gene_type,
            normalized_gene,
            normalized_gene_type,
            submitter_id,
            submitter_name,
            submitter_country_code,
            submitter_country_name,
            rcv,
            scv,
            significance,
            normalized_significance,
            last_eval,
            review_status,
            star_level,
            condition_name,
            condition_xrefs,
            method,
            normalized_method,
            comment,
        ))

    return submissions

def import_file(filename):
    matches = re.fullmatch(r'ClinVarFullRelease_(\d\d\d\d-\d\d).xml', basename(filename))
    if matches:
        print('Importing ' + filename)
    else:
        print('Skipped unrecognized filename ' + filename)
        return

    date = matches.group(1)

    #hack the ClinVar XML file into pieces to parse it in parallel
    with open(filename, 'r+b') as f:
        clinvarsets = re.findall(b'<ClinVarSet .+?</ClinVarSet>', mmap(f.fileno(), 0), re.DOTALL)
    submission_sets = Pool().map(partial(get_submissions, date), clinvarsets)
    submissions = [submission for submission_set in submission_sets for submission in submission_set]

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
            t2.normalized_significance,
            t2.star_level,
            t2.condition_name,
            t2.normalized_method,
            CASE
                WHEN t1.scv=t2.scv THEN -1

                WHEN t1.significance=t2.significance THEN 0
                WHEN t1.normalized_significance="not provided" OR t2.normalized_significance="not provided" THEN 0

                WHEN t1.normalized_significance=t2.normalized_significance THEN 1

                WHEN t1.normalized_significance="benign" AND t2.normalized_significance="likely benign" THEN 2
                WHEN t1.normalized_significance="likely benign" AND t2.normalized_significance="benign" THEN 2
                WHEN t1.normalized_significance="pathogenic" AND t2.normalized_significance="likely pathogenic" THEN 2
                WHEN t1.normalized_significance="likely pathogenic" AND t2.normalized_significance="pathogenic" THEN 2

                WHEN t1.normalized_significance IN ("benign", "likely benign") AND t2.normalized_significance="uncertain significance" THEN 3
                WHEN t1.normalized_significance="uncertain significance" AND t2.normalized_significance IN ("benign", "likely benign") THEN 3

                WHEN t1.normalized_significance IN ("benign", "likely benign", "uncertain significance") AND t2.normalized_significance IN ("pathogenic", "likely pathogenic") THEN 5
                WHEN t1.normalized_significance IN ("pathogenic", "likely pathogenic") AND t2.normalized_significance IN ("benign", "likely benign", "uncertain significance") THEN 5

                ELSE 4
            END AS conflict_level
        FROM submissions t1 INNER JOIN submissions t2
        ON t1.date=? AND t1.date=t2.date AND t1.variant_name=t2.variant_name
    ''', [date])

    db.commit()
    db.close()

if __name__ == '__main__':
    if len(argv) < 2:
        print('Usage: ./import-clinvar-xml.py ClinVarFullRelease_<year>-<month>.xml ...')
        exit()

    create_tables()
    for filename in argv[1:]:
        import_file(filename)
