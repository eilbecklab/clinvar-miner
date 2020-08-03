all: mondo
	./import-all-clinvar-xmls.sh
	./create-indexes.py

countries:
	curl https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/organization_summary.txt > organization_summary.txt
	./get-submitter-info.py

mondo:
	curl -L http://purl.obolibrary.org/obo/mondo.owl > mondo.owl

latest: mondo
	./import-latest-clinvar-xml.sh
	./create-indexes.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal

test:
	./mondo_test.py
