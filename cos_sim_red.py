#!/usr/bin/env python

import argparse
from map_reduce_utils import reducer_stream


KEYS = ['file1', 'file2']
VALUES = ['term']


def reduce_cosine_similarity(precision, input=reducer_stream(KEYS, VALUES)):
    """
    (file1 file2) (tfidf1*tfidf2) --> (file1 file2) (cosine_similarity(f1, f2))

    sums up the products of the tfidf values of words common between every
    pair of documents to produce the cosine similarity of the two documents
    """
    for key, key_stream in input:
        sum_for_docs = 0
        for value in key_stream:
            term = value['term']
            sum_for_docs += float(term)
            print_result(key['file1'], key['file2'], sum_for_docs, precision)


def print_result(doc1, doc2, sum_for_docs, precision):
    print '{0} {1}\t{2:.{3}f}'.format(doc1, doc2, sum_for_docs, precision)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--precision', '-p', dest='precision')
    precision = int(parser.parse_args().precision)
    reduce_cosine_similarity(precision)
