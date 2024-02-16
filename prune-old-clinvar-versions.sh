#!/bin/bash

echo Pruning old ClinVar versions
year=$(date +%Y)
for table in submissions comparisons mondo_clinvar_relationships; do
    # echo "DELETE FROM $table WHERE date NOT LIKE '$year-%' AND date NOT LIKE '%-12-%'" | sqlite3 clinvar.db
    echo "DELETE FROM $table WHERE date != (SELECT MAX(date) FROM submissions)" | sqlite3 clinvar.db
done
