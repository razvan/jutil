#!/bin/sh

./bin/wordcount.py 'dewiki/txt/*/*.bz2' doc > dewiki/wordcount/docs

./bin/wordcount.py 'dewiki/txt/*/*.bz2' dfreq | gzip -c > dewiki/wordcount/dfreq.gz

TOTALDOCS=`cat dewiki/wordcount/docs`

zcat dewiki/wordcount/dfreq.gz | perl -anle "$,=\"\\t\"; print 100.0*\$F[1]/$TOTALDOCS, \$F[0];" | sort -g -t\t | perl -anle '$,="\t"; print reverse @F;' | gzip -c > dewiki/wordcount/reldfreq.gz

