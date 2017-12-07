#!/bin/bash

function import {
    url=$1
    filename=$2
    echo Downloading $url
    curl $url | gunzip > $filename 2> /dev/null
    if [ -s $filename ]; then
        if ./import-clinvar-xml.py $filename; then
            ./create-current-tables.py
            rm $filename
        fi
    fi
    echo
}

filename=ClinVarFullRelease_$(date +%Y-%m).xml
import ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/$filename.gz $filename
