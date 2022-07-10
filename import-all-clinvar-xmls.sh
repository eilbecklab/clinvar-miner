#!/bin/bash

function import {
    url=$1
    filename=$2
    echo Downloading $url
    curl $url | gunzip > $filename 2> /dev/null
    if [ -s $filename ]; then
        ./import-clinvar-xml.py $filename
    fi
    rm $filename
    echo
}

# Import only the December release from past years
for year in $(seq 2012 $(expr $(date +%Y) - 1)); do
    filename=ClinVarFullRelease_$year-12.xml
    import https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/archive/$year/$filename.gz $filename
done

# Import the releases from every month of the current year
year=$(date +%Y)
for month in $(seq -f '%02g' 1 $(date +%m)); do
    filename=ClinVarFullRelease_$year-$month.xml
    import https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/$filename.gz $filename
done
