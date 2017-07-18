all:
	./import-all-clinvar-xmls.sh

countries:
	./scrape-submitter-info.py

latest:
	./import-latest-clinvar-xml.sh

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
