#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def map_corpus_frequency(input=sys.stdin, output=sys.stdout):
    """
    (word filename) (n N) --> (word) (filename n N 1)

    emits a line for each unique word in each file to be consumed
    by corp_freq_red to find the number of occurences of each
    unique word throughout the entire corpus.
    """
    for in_key, in_value in mru.json_loader(input):
        out_key = {'word': in_key['word']}
        out_value = {'filename': in_key['filename'],
                     'word_freq': in_value['word_freq'],
                     'doc_size': in_value['doc_size'],
                     'count': 1}
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_corpus_frequency()
