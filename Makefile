all: mondo
	./import-all-clinvar-xmls.sh
	./create-current-tables.py

countries:
	wget -O organization_summary.txt ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/organization_summary.txt
	./get-submitter-info.py

mondo:
	wget -O mondo.owl http://purl.obolibrary.org/obo/mondo.owl

latest: mondo
	./import-latest-clinvar-xml.sh
	./create-current-tables.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
