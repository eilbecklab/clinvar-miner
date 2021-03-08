#!/usr/bin/env python3

import csv
from pycountry import countries

additional_common_names = {
    'iran': 'IRN',
    'palestine': 'PSE',
    'north korea': 'PRK',
    'russia': 'RUS',
    'south korea': 'KOR',
    'syria': 'SYR',
}

def lookup_country_code(country):
    country = country.strip()
    if not country:
        return ''

    # Try the pycountry database first
    try:
        return countries.lookup(country).alpha_3
    except LookupError:
        pass

    # If that fails, try common names that are not in the pycountry database
    country = country.lower()
    if country in additional_common_names:
        return additional_common_names[country]

    return ''

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
        country_code = lookup_country_code(row[4])
        if not country_code and submitter_id in submitter_info:
            country_code = submitter_info[submitter_id][1]
        submitter_info[submitter_id] = [submitter_name, country_code]

with open('submitter_info.tsv', 'w') as f:
    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
    for submitter_id in sorted(submitter_info.keys(), key=int):
        writer.writerow([submitter_id] + submitter_info[submitter_id])
