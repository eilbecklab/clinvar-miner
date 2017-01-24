#!/usr/bin/env python3

import html5lib
import re
import sqlite3
from sys import stdout
from urllib.request import urlopen

ns = {'html': 'http://www.w3.org/1999/xhtml'}

submitter_urls = {}
with urlopen('https://www.ncbi.nlm.nih.gov/clinvar/docs/submitter_list/') as f:
    root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
    for el in root.findall('.//html:table[@id="all_sub_linkify"]/html:tbody/html:tr/html:td/html:a', ns):
        submitter_urls[el.text] = el.attrib['href']

db = sqlite3.connect('clinvar-conflicts.db', timeout=600)
cursor = db.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS submitter_info (
        id TEXT,
        name TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        country TEXT,
        PRIMARY KEY (id)
    )
''')

count = 0
stdout.write('Importing ' + str(len(submitter_urls)) + ' addresses...\n')

for name, url in submitter_urls.items():
    with urlopen('https://www.ncbi.nlm.nih.gov' + url) as f:
        submitter_id = re.match('/clinvar/submitters/(.+)/', url).group(1)
        root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
        contact_info_el = root.find('.//html:div[@id="maincontent"]//html:div[@class="submitter_main indented"]//html:div[@class="indented"]/html:div[@class="indented"]', ns)
        if contact_info_el: #the "ClinVar" submitter has no contact information
            contact_info = list(contact_info_el.itertext())[1:]
            contact_info = list(filter(lambda info: not re.match('http://|https://|Organization ID:', info), contact_info))
            city = contact_info[-3] if len(contact_info) >= 3 else ''
            state = contact_info[-2] if len(contact_info) >= 2 else ''
            if len(contact_info) >= 1:
                country_and_zip = re.match('(.+) - (.+)', contact_info[-1])
                zip_code = country_and_zip.group(2) if country_and_zip else ''
                country = country_and_zip.group(1) if country_and_zip else contact_info[-1]
            else:
                zip_code = ''
                country = ''
        else:
            city = ''
            state = ''
            zip_code = ''
            country = ''
        cursor.execute(
            'INSERT INTO submitter_info VALUES (?,?,?,?,?,?)', (submitter_id, name, city, state, zip_code, country)
        )
        count += 1
        stdout.write('\r\033[K' + str(count) + '\t' + name)

stdout.write('\r\033[K')
db.commit()
db.close()
