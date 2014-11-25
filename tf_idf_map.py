#!/usr/bin/env python

import sys
from math import log

"""
(word file_name) (n N m) --> (word file_name) (tfidf)

computes the tf-idf metric for each word in each file in the corpus
which is defined as the term frequency multiplied by the inverse document
frequency. The term frequency is what porportion of the words in
the document are a given word. The inverse document frequency is the
number of documents in the corpus that the word appears.
"""

if len(sys.argv) == 1:
    err_msg = "{0} requires the size of the corpus as an argument"
    raise Exception(err_msg.format(sys.argv[0]))

corpora_size = sys.argv[1]


for line in sys.stdin:
    key, value = line.strip().split('\t')
    n, N, m = value.strip().split()
    n = int(n)
    N = int(N)
    m = int(m)
    D = corpora_size
    tf = float(n) / float(N)
    idf = log((float(D) / float(m)), 10)
    tfidf = tf * idf
    print '%s\t%.16f' % (key, tfidf)
