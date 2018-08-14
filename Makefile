all: mondo
	./import-all-clinvar-xmls.sh
	./create-current-tables.py

countries:
	./scrape-submitter-info.py

mondo:
	wget -O mondo.owl http://purl.obolibrary.org/obo/mondo.owl

latest: mondo
	./import-latest-clinvar-xml.sh
	./create-current-tables.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
