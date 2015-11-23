#! /bin/sh
cat output/part-* | sort -rnk 2 | head -10
