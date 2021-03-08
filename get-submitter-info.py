#!/usr/bin/env python3

import csv
import html5lib
from pycountry import countries
from urllib.error import URLError
from urllib.request import urlopen

ns = {'html': 'http://www.w3.org/1999/xhtml'}

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

def scrape_country_code(submitter_id):
    try:
        f = urlopen(f'https://www.ncbi.nlm.nih.gov/clinvar/submitters/{submitter_id}/')
        root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
        submitter_el = root.find('.//html:div[@id="maincontent"]//html:div[@class="submitter_main indented"]', ns)
        contact_info_el = submitter_el.find('.//html:div[@class="indented"]/html:div[@class="indented"]', ns)

        for line in reversed(list(contact_info_el.itertext())):
            for field in line.split(' - '):
                code = lookup_country_code(field)
                if code:
                    return code
    except URLError:
        # no internet connection
        pass
    except:
        print('The country scraping code no longer works because the ClinVar website has changed.')

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
        # First look for the country code in the official ClinVar spreadsheet,
        # then in the current ClinVar Miner spreadsheet, then on the ClinVar
        # website
        country_code = lookup_country_code(row[4])
        if not country_code and submitter_id in submitter_info:
            country_code = submitter_info[submitter_id][1]
        if not country_code:
            country_code = scrape_country_code(submitter_id)
        submitter_info[submitter_id] = [submitter_name, country_code]

with open('submitter_info.tsv', 'w') as f:
    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
    for submitter_id in sorted(submitter_info.keys(), key=int):
        writer.writerow([submitter_id] + submitter_info[submitter_id])
