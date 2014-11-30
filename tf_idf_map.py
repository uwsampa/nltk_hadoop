#!/usr/bin/env python

import sys
import argparse
from math import log

"""
(word file_name) (n N m) --> (word file_name) (tfidf)

computes the tf-idf metric for each word in each file in the corpus
which is defined as the term frequency multiplied by the inverse document
frequency. The term frequency is what porportion of the words in
the document are a given word. The inverse document frequency is the
number of documents in the corpus that the word appears.
"""

parser = argparse.ArgumentParser()
parser.add_argument('--corpus-size', '-s', dest='corpus_size')
parser.add_argument('--precision', '-p', dest='precision')
args = parser.parse_args()
corpus_size = args.corpus_size
precision = int(args.precision)

template = '{0}\t{1:.{2}f}'


for line in sys.stdin:
    key, value = line.strip().split('\t')
    n, N, m = value.strip().split()
    n = int(n)
    N = int(N)
    m = int(m)
    D = corpus_size
    tf = float(n) / float(N)
    idf = log((float(D) / float(m)), 10)
    tfidf = tf * idf
    print template.format(key, tfidf, precision)
