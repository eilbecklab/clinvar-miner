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

for year in $(seq 2012 $(expr $(date +%Y) - 1)); do
    for month in $(seq -f '%02g' 1 12); do
        filename=ClinVarFullRelease_$year-$month.xml
        import ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/archive/$year/$filename.gz $filename
    done
done

year=$(date +%Y)
for month in $(seq -f '%02g' 1 $(date +%m)); do
    filename=ClinVarFullRelease_$year-$month.xml
    import ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/$filename.gz $filename
done
