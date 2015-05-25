#!/usr/bin/env python

from __future__ import print_function
from sys import stdout
import map_reduce_utils as mru


def reduce_cosine_similarity(input=mru.reducer_stream(), output=stdout):
    """
    (file1 file2) (tfidf1*tfidf2) --> (file1 file2) (cosine_similarity(f1, f2))

    sums up the products of the tfidf values of words common between every
    pair of documents to produce the cosine similarity of the two documents
    """
    for in_key, key_stream in input:
        sum_for_docs = 0
        for in_value in key_stream:
            sum_for_docs += in_value['product']
        out_key = {'file1': in_key['file1'], 'file2': in_key['file2']}
        out_value = {'cos_similarity': sum_for_docs}
        mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
    reduce_cosine_similarity()
