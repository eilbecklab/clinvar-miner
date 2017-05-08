all:
	./import-all-clinvar-xmls.sh
	./import-submitter-info.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
