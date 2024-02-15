#!/bin/bash

function import {
    year=$1
    month=$2

    filename=ClinVarFullRelease_$year-$month.xml

    # Try downloading from the root directory first
    url=https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_xml_old_format/$filename.gz
    echo Downloading $url
    curl $url | gunzip > $filename 2> /dev/null

    # If that fails, try downloading from the archive directory
    if [ ! -s $filename ]; then
        url=https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_xml_old_format/archive/$year/$filename.gz
        echo Downloading $url
        curl $url | gunzip > $filename 2> /dev/null
    fi

    # If we successfully got the file, import it!
    if [ -s $filename ]; then
        ./import-clinvar-xml.py $filename
    fi

    # Delete the file and output a blank line
    rm $filename
    echo
}

# Import only the December release from past years
for year in $(seq 2012 $(expr $(date +%Y) - 1)); do
    import $year 12
done

# Import the releases from every month of the current year
year=$(date +%Y)
for month in $(seq -f '%02g' 1 $(date +%m)); do
    import $year $month
done
