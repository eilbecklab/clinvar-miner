#!/usr/bin/env python3

import html5lib
import re
import sqlite3
from pycountry import countries
from sys import stdout
from urllib.error import HTTPError
from urllib.request import urlopen

ns = {'html': 'http://www.w3.org/1999/xhtml'}

db = sqlite3.connect('clinvar.db', timeout=600)
cursor = db.cursor()

submitter_ids = list(map(
    lambda row: row[0],
    cursor.execute('SELECT DISTINCT submitter_id FROM submissions WHERE submitter_id != 0')
))

cursor.execute('''
    CREATE TABLE IF NOT EXISTS submitter_info (
        id INTEGER,
        name TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        country TEXT,
        country_code TEXT,
        PRIMARY KEY (id)
    )
''')

cursor.execute('CREATE INDEX IF NOT EXISTS country_index ON submitter_info (country)')
cursor.execute('CREATE INDEX IF NOT EXISTS country_code_index ON submitter_info (country_code)')

count = 0
stdout.write('Importing information on ' + str(len(submitter_ids)) + ' submitters...\n')

for submitter_id in submitter_ids:
    try:
        with urlopen('https://www.ncbi.nlm.nih.gov/clinvar/submitters/' + str(submitter_id) + '/') as f:
            count += 1
            stdout.write('\r\033[K' + str(count) + '\tSubmitter ' + str(submitter_id))
            root = html5lib.parse(f, transport_encoding=f.info().get_content_charset())
            submitter_el = root.find('.//html:div[@id="maincontent"]//html:div[@class="submitter_main indented"]', ns)
            name = submitter_el.find('./html:h2', ns).text
            contact_info_el = submitter_el.find('.//html:div[@class="indented"]/html:div[@class="indented"]', ns)
            city = ''
            state = ''
            zip_code = ''
            country = ''
            country_code = ''
            if contact_info_el: #the "ClinVar" submitter has no contact information
                contact_info = list(contact_info_el.itertext())[1:]
                contact_info = list(filter(lambda info: not re.match('http://|https://|Organization ID:', info), contact_info))
                city = contact_info[-3] if len(contact_info) >= 3 else ''
                state = contact_info[-2] if len(contact_info) >= 2 else ''
                if len(contact_info) >= 1:
                    country_and_zip = re.match('(.+) - (.+)', contact_info[-1])
                    zip_code = country_and_zip.group(2) if country_and_zip else ''
                    country = country_and_zip.group(1) if country_and_zip else contact_info[-1]
                    try:
                        country_code = countries.lookup(country).alpha_3
                    except LookupError: #not a real country
                        country = ''
            cursor.execute(
                'INSERT OR REPLACE INTO submitter_info VALUES (?,?,?,?,?,?,?)',
                (int(submitter_id), name, city, state, zip_code, country, country_code)
            )
    except HTTPError as err:
        if err.code == 404:
            print('\r\033[KNo information for submitter ' + str(submitter_id))
            continue
        raise err

stdout.write('\r\033[K')
db.commit()
db.close()
