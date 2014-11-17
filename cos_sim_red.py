#!/usr/bin/env python
import sys

cur_sum = 0
cur_docs = (None, None)  # will become (doc1, doc2)

for line in sys.stdin:
    key, value = line.strip().split('\t')
    doc1, doc2 = key.strip().split()
    product = float(value)
    if (doc1, doc2) == cur_docs:
        cur_sum += product
    else:
        if cur_docs[0] is not None and cur_docs[1] is not None:
            print '%s %s\t%.16f' % (cur_docs[0], cur_docs[1], cur_sum)
        cur_docs = (doc1, doc2)
        cur_sum = 0

if cur_docs[0] is not None and cur_docs[1] is not None:
    print '%s %s\t%.16f' % (cur_docs[0], cur_docs[1], cur_sum)
