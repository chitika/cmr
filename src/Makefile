all:
	gcc -D_GNU_SOURCE -std=c99 -O2 cmr-merge.c -o cmr-merge
	gcc -D_GNU_SOURCE -std=c99 -O2 cmr-bucket.c -o cmr-bucket
	gcc -D_GNU_SOURCE -std=c99 -O2 cmr-pipe.c -o cmr-pipe
	gcc -D_GNU_SOURCE -std=c99 -O2 chunky.c -o chunky

clean:
	rm cmr-merge cmr-bucket cmr-pipe chunky

