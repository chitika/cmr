all:
	gcc -D_GNU_SOURCE -std=c99 -O2 src/cmr-merge.c -o $(INST_BIN)/cmr-merge
	gcc -D_GNU_SOURCE -std=c99 -O2 src/cmr-bucket.c -o $(INST_BIN)/cmr-bucket
	gcc -D_GNU_SOURCE -std=c99 -O2 src/cmr-pipe.c -o $(INST_BIN)/cmr-pipe
	gcc -D_GNU_SOURCE -std=c99 -O2 src/chunky.c -o $(INST_BIN)/chunky
