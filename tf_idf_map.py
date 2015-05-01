#!/usr/bin/env python

from __future__ import print_function
import sys
import argparse
import map_reduce_utils as mru
from math import log


def map_tf_idf(corpus_size, input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (n N m) --> (word file_name) (tfidf)

    computes the tf-idf metric for each word in each file in the corpus
    which is defined as the term frequency multiplied by the inverse document
    frequency. The term frequency is what porportion of the words in
    the document are a given word. The inverse document frequency is the
    number of documents in the corpus that the word appears.
    """

    for in_key, in_value in mru.json_loader(input):
        n = in_value['word_freq']
        N = in_value['doc_size']
        m = in_value['corp_freq']
        D = in_key['corpus_size']
        tf = float(n) / float(N)
        idf = log((float(D) / float(m)), 10)
        tfidf = tf * idf
        # in_key == out_key
        mru.reducer_emit(in_key, {'tfidf': tfidf}, output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    corpus_size = args.s
    map_tf_idf(corpus_size)
