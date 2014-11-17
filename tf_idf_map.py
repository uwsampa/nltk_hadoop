#!/usr/bin/env python
import sys
from math import log


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
