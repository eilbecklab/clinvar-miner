all: gene_summary
	./import-all-clinvar-xmls.sh
	./create-current-tables.py

gene_summary:
	wget -O gene_specific_summary.tsv ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/gene_specific_summary.txt

countries:
	./scrape-submitter-info.py

latest: gene_summary
	./import-latest-clinvar-xml.sh
	./create-current-tables.py

clean:
	rm -f clinvar.db
	rm -f clinvar.db-journal
