#!/usr/bin/env python

from __future__ import print_function
from sys import stdout
import map_reduce_utils as mru


def reduce_corpus_frequency(input=mru.reducer_stream(), output=stdout):
    """
    (word) (filename n N 1) --> (word filename) (n N m)

    sums up the number of occurences of each unique word throughout
    the corpus and emits this sum for each document that the word
    occurs in.
    """
    for in_key, key_stream in input:
        corpus_frequency = 0
        values = []
        for in_value in key_stream:
            corpus_frequency += in_value['count']
            values.append(in_value)
        for value in values:
            out_key = {'word': in_key['word'], 'filename': value['filename']}
            out_value = {'word_freq': value['word_freq'],
                         'doc_size': value['doc_size'],
                         'corp_freq': corpus_frequency}
            mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
    reduce_corpus_frequency()
