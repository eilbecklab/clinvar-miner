#!/usr/bin/env python3

import csv
from pycountry import countries

submitter_info = {}
for row in csv.reader(open('submitter_info.tsv'), delimiter='\t'):
    submitter_id = row[0]
    submitter_name = row[1]
    country_code = row[2]
    submitter_info[submitter_id] = [submitter_name, country_code]

with open('organization_summary.txt') as f:
    next(f) #skip header
    for row in csv.reader(f, delimiter='\t'):
        submitter_name = row[0]
        submitter_id = row[1]
        try:
            country_code = countries.lookup(row[4]).alpha_3
        except LookupError: #not a real country
            country_code = ''
        if not country_code and submitter_id in submitter_info:
            country_code = submitter_info[submitter_id][1]
        submitter_info[submitter_id] = [submitter_name, country_code]

with open('submitter_info.tsv', 'w') as f:
    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
    for submitter_id in sorted(submitter_info.keys(), key=int):
        writer.writerow([submitter_id] + submitter_info[submitter_id])
