#!/usr/bin/env python
import sys

"""
(file1 file2) (tfidf1*tfidf2) --> (file1 file2) (cosine_similarity(f1, f2))

sums up the products of the tfidf values of words common between every
pair of documents to produce the cosine similarity of the two documents
"""

cur_sum = 0
cur_docs = (None, None)  # will become (doc1, doc2)


def print_result(doc1, doc2, sum_for_docs):
    print '%s %s\t%.16f' % (doc1, doc2, sum_for_docs)

for line in sys.stdin:
    key, value = line.strip().split('\t')
    doc1, doc2 = key.strip().split()
    product = float(value)
    if (doc1, doc2) == cur_docs:
        cur_sum += product
    else:
        if cur_docs[0] is not None and cur_docs[1] is not None:
            print_result(cur_docs[0], cur_docs[1], cur_sum)
        cur_docs = (doc1, doc2)
        cur_sum = 0

if cur_docs[0] is not None and cur_docs[1] is not None:
    print_result(cur_docs[0], cur_docs[1], cur_sum)
