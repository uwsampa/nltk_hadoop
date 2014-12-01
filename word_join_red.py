#!/usr/bin/env python

import map_reduce_utils as mr_util
import argparse

"""
(word) (file_name tfidf) --> (word) (file1 file2 tfidf1*tfidf2)

for each word, if two distinct documents both contain that word,
a line is emitted containing the product of the tfidf scores of that
word in both documents.

This is the first step in computing the pairwise dot product of the tf-idf
vectors between all documents, where the corresponding elements for every
pair of documents are multiplied together.
"""

parser = argparse.ArgumentParser()
parser.add_argument('--precision', '-p', dest='precision')
precision = int(parser.parse_args().precision)

key_names = ['word']
value_names = ['filename', 'tfidf']


def print_results(values, word, precision=precision):
    template = '{0}\t{1} {2} {3:.{4}f}'
    for doc1 in values:
        for doc2 in values:
            if doc1['filename'] != doc2['filename']:
                product = float(doc1['tfidf']) * float(doc2['tfidf'])
                print template.format(word,
                                      doc1['filename'],
                                      doc2['filename'],
                                      product, precision)


for key, key_stream in mr_util.reducer_stream(key_names, value_names):
    values = []
    for value in key_stream:
        values.append(value)
    print_results(values, key['word'])
