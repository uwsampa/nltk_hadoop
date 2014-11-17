#!/usr/bin/env python
import sys

for line in sys.stdin:
    key, value = line.strip().split('\t')
    word, doc = key.strip().split()
    print '%s\t%s %s' % (word, doc, value)
