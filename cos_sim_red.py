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
kv_convert = mr_util.KeyValueToDict(keys, values)


def print_result(doc1, doc2, sum_for_docs, precision=precision):
    print '{0} {1}\t{2:.{3}f}'.format(doc1, doc2, sum_for_docs, precision)


for key_stream in mr_util.reducer_stream():
    sum_for_docs = 0
    for kv_pair in key_stream:
        kv_dict = kv_convert.to_dict(kv_pair)
        term = kv_dict['value']['term']
        sum_for_docs += float(term)
    print_result(kv_dict['key']['file1'], kv_dict['key']['file2'], sum_for_docs)
