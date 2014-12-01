#!/usr/bin/env python

import argparse
import map_reduce_utils as mr_util

"""
(file1 file2) (tfidf1*tfidf2) --> (file1 file2) (cosine_similarity(f1, f2))

sums up the products of the tfidf values of words common between every
pair of documents to produce the cosine similarity of the two documents
"""

parser = argparse.ArgumentParser()
parser.add_argument('--precision', '-p', dest='precision')
precision = int(parser.parse_args().precision)

keys = ['file1', 'file2']
values = ['term']


def print_result(doc1, doc2, sum_for_docs, precision=precision):
    print '{0} {1}\t{2:.{3}f}'.format(doc1, doc2, sum_for_docs, precision)


for key, key_stream in mr_util.reducer_stream(keys, values):
    sum_for_docs = 0
    for value in key_stream:
        term = value['term']
        sum_for_docs += float(term)
    print_result(key['file1'], key['file2'], sum_for_docs)
