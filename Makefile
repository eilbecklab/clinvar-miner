all:
	./scrape-submitter-info.py
	./import-all-clinvar-xmls.sh

latest:
	./scrape-submitter-info.py
	./import-latest-clinvar-xml.sh

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
