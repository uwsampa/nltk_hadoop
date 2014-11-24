#!/usr/bin/env python

import sys

"""
(word file_name) (n N) --> (word) (file_name n N 1)

emits a line for each unique word in each file to be consumed
by corp_freq_red to find the number of occurences of each
unique word throughout the entire corpus.
"""

for line in sys.stdin:
    key, value = line.strip().split('\t')
    word, docname = key.strip().split()
    print '%s\t%s %s %s' % (word, docname, value, 1)
