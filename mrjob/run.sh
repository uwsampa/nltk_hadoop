#!/bin/bash

# exit on error
set -e

# load environment variables for patents
source /shared/patents/settings.sh

# choose a unique path prefix
PREFIX=hdfs:///user/$USER/patents-run-$(date +%s)

# # If desired, say explicitly what we want to clean up.
# # Choices include ALL, LOCAL_SCRATCH, LOGS, NONE, REMOTE_SCRATCH, SCRATCH, JOB, IF_SUCCESSFUL, JOB_FLOW.
# # default is ALL.
# # generally this will be commented out except for debugging.
# CLEANUP="--cleanup=NONE"

# count patents
python PatentCount.py --runner=hadoop --no-output $CLEANUP --setup='source /shared/patents/settings.sh' hdfs:///patents/claims_splits --output-dir=$PREFIX/patent-count

# get count
NUM_PATENTS=$(hadoop fs -cat $PREFIX/patent-count/part-00000 | cut -f2)
## NUM_PATENTS=57860

# compute TF-IDF
python TFIDF.py --runner=hadoop --jobconf mapreduce.job.reduces=48 --no-output $CLEANUP --setup='source /shared/patents/settings.sh' --num-patents $NUM_PATENTS hdfs:///patents/claims_splits --output-dir=$PREFIX/tfidf

# unused; better to do in C++
#python PatentMap.py --runner=hadoop --no-output $CLEANUP --setup='source /shared/patents/settings.sh' $PREFIX/tfidf --output-dir=$PREFIX/pmap

# unused; better to do in C++
#python BigramMap.py --runner=hadoop --no-output $CLEANUP --setup='source /shared/patents/settings.sh' $PREFIX/tfidf --output-dir=$PREFIX/bmap

# much better to do in C++
#python CosineSimilarity.py --runner=hadoop --jobconf mapreduce.job.reduces=48 --no-output $CLEANUP --setup='source /shared/patents/settings.sh' $PREFIX/tfidf --output-dir=$PREFIX/similarity

# # compute TF-IDF and cosine similarity
# python CosineSimilarity.py --runner hadoop --no-output --setup 'source /shared/patents/settings.sh' hdfs:///patents/claims_splits --output-dir $PREFIX/similarity --num-patents $NUM_PATENTS

