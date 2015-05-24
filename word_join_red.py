#!/usr/bin/env python

from __future__ import print_function
import argparse
import sys
from map_reduce_utils import reducer_stream


KEYS = ['word']
VALUES = ['filename', 'tfidf']


def reduce_word_join(precision,
                     input=reducer_stream(KEYS, VALUES),
                     output=sys.stdout):
    """
    (word) (file_name tfidf) --> (word) (file1 file2 tfidf1*tfidf2)

    for each word, if two distinct documents both contain that word,
    a line is emitted containing the product of the tfidf scores of that
    word in both documents.

    This is the first step in computing the pairwise dot product of the tf-idf
    vectors between all documents, where the corresponding elements for every
    pair of documents are multiplied together.
    """

    for key, key_stream in input:
        values = []
        for value in key_stream:
            values.append(value)
        print_results(values, key['word'], precision, output)


def print_results(values, word, precision, output):
    template = '{0}\t{1} {2} {3:.{4}f}'
    for doc1 in values:
        for doc2 in values:
            if doc1['filename'] != doc2['filename']:
                product = float(doc1['tfidf']) * float(doc2['tfidf'])
                result = template.format(word, doc1['filename'],
                                         doc2['filename'], product, precision)
                print(result, file=output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--precision', '-p', dest='precision')
    precision = int(parser.parse_args().precision)
    reduce_word_join(precision)
