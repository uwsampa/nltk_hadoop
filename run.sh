#!/bin/bash

source /shared/patents/nltk-hadoop/settings.sh
source /shared/patents/settings.sh
./mapred_tfidf.py -i hdfs:///patents/claims_splits -o hdfs:///patents/output/similarities -s stopwords.txt
