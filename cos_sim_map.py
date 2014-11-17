#!/usr/bin/env python
import sys

for line in sys.stdin:
    key, value = line.strip().split('\t')
    doc1, doc2, product = value.strip().split()
    product = float(product)
    print '%s %s\t%.16f' % (doc1, doc2, product)
