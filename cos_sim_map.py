#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def map_cosine_similarity(input=sys.stdin, output=sys.stdout):
    """
    (word) (file1 file2 tfidf1*tfidf2) --> (file1 file2) (tfidf1*tfidf2)

    for each word common to two documents, removes the word from the
    key/value pair and replaces it with the two filenames so that we can
    sum up the values for each pair of documents in the reducer.
    """
    for in_key, in_value in mru.json_loader(input):
        file1 = in_value['file1']
        file2 = in_value['file2']
        # we want to ensure that (file1 file2) and (file2 file1) get
        # sent to the same reducer, so we order them alphabetically
        if file1 > file2:
            file1, file2 = file2, file1
        out_key = {'file1': file1, 'file2': file2}
        out_value = {'product': in_value['product']}
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_cosine_similarity()
