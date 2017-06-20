#!/usr/bin/env python3

import csv
import html5lib
import re
from pycountry import countries
from sys import stdout
from urllib.error import HTTPError
from urllib.request import urlopen

ns = {'html': 'http://www.w3.org/1999/xhtml'}

with urlopen('https://www.ncbi.nlm.nih.gov/clinvar/docs/submitter_list/') as f:
    root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
    submitter_ids = list(map(
        lambda a: a.attrib['href'].split('/')[-2],
        root.findall('.//html:table[@id="all_sub_linkify"]//html:td[1]//html:a', ns)
    ))

count = 0
stdout.write('Downloading information about ' + str(len(submitter_ids)) + ' submitters...\n')

submitter_info = {}
for row in csv.reader(open('submitter_info.tsv'), delimiter='\t'):
    submitter_id = row[0]
    submitter_name = row[1]
    country_code = row[2]
    submitter_info[submitter_id] = [submitter_name, country_code]

for submitter_id in submitter_ids:
    try:
        with urlopen('https://www.ncbi.nlm.nih.gov/clinvar/submitters/' + str(submitter_id) + '/') as f:
            count += 1
            stdout.write('\r\033[K' + str(count) + '\tSubmitter ' + str(submitter_id))

            if submitter_id in submitter_info:
                submitter_name = submitter_info[submitter_id][0]
                country_code = submitter_info[submitter_id][1]
            else:
                submitter_name = ''
                country_code = ''

            root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
            submitter_el = root.find('.//html:div[@id="maincontent"]//html:div[@class="submitter_main indented"]', ns)
            name_el = submitter_el.find('./html:h2', ns)
            contact_info_el = submitter_el.find('.//html:div[@class="indented"]/html:div[@class="indented"]', ns)

            if name_el != None and name_el.text: #submitter 1 has no name
                submitter_name = re.sub(r'\s+', ' ', name_el.text.strip())

            if contact_info_el != None: #submitter 1 has no contact information
                contact_info = list(contact_info_el.itertext())[1:]
                contact_info = list(filter(lambda info: not re.match('http://|https://|Organization ID:', info), contact_info))
                if len(contact_info) >= 1:
                    country_and_zip = re.match('(.+) - (.+)', contact_info[-1])
                    country_name = country_and_zip.group(1) if country_and_zip else contact_info[-1]
                    try:
                        country_code = countries.lookup(country_name).alpha_3
                    except LookupError: #not a real country
                        pass

            submitter_info[submitter_id] = [submitter_name, country_code]
    except HTTPError as err:
        if err.code == 404:
            print('\r\033[KNo information for submitter ' + str(submitter_id))
            continue
        raise err

stdout.write('\r\033[K')

with open('submitter_info.tsv', 'w') as f:
    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
    for submitter_id in sorted(submitter_info.keys(), key=int):
        writer.writerow([submitter_id] + submitter_info[submitter_id])
