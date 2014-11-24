#!/usr/bin/env python
import sys

"""
(word) (file1 file2 tfidf1*tfidf2) --> (file1 file2) (tfidf1*tfidf2)

for each word common to two documents, removes the word from the
key/value pair and replaces it with the two filenames so that we can
sum up the values for each pair of documents in the reducer.
"""

for line in sys.stdin:
    key, value = line.strip().split('\t')
    doc1, doc2, product = value.strip().split()

    # we want to ensure that (doc1 doc2) and (doc2 doc1) get
    # sent to the same reducer, so we order them alphabetically
    if doc1 > doc2:
        doc1, doc2 = doc2, doc1

    print '%s %s\t%s' % (doc1, doc2, product)
