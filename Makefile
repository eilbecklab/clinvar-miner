all:
	./import-all-clinvar-xmls.sh
	./create-current-tables.py

countries:
	./scrape-submitter-info.py

latest:
	./import-latest-clinvar-xml.sh
	./create-current-tables.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
