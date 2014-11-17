#!/usr/bin/env python

import sys

for line in sys.stdin:
    file_name, words = line.strip().split('\t')
    for word in words.strip().split():
        print '%s %s\t%s' % (word, file_name, 1)
