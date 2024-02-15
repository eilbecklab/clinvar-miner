#!/bin/bash

filename=ClinVarFullRelease_00-latest.xml
url=https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_xml_old_format/$filename.gz

echo Downloading $url
curl $url | gunzip > $filename 2> /dev/null
./import-clinvar-xml.py $filename
