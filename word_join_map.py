#!/usr/bin/env python
import sys

"""
(word file_name) (tfidf) --> (word) (file_name tfidf)

emits a line for each word in each file with the word as a key
and the filename and tfidf score as the value
"""

for line in sys.stdin:
    key, value = line.strip().split('\t')
    word, doc = key.strip().split()
    print '%s\t%s %s' % (word, doc, value)
