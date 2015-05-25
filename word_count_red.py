#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def reduce_word_count(input=mru.reducer_stream(), output=sys.stdout):
    """
    (file_name) (word word_freq) --> (word file_name) (n N)

    sums up the total number of words in each document and emits
    that sum for each word along with the number of occurences of that
    word in the given document
    """

    for in_key, key_stream in input:
        doc_size = 0
        values = []
        for in_value in key_stream:
            values.append(in_value)
            doc_size += in_value['word_freq']
        for value in values:
            out_key = {'word': value['word'], 'filename': in_key['filename']}
            out_value = {'word_freq': value['word_freq'], 'doc_size': doc_size}
            mru.reducer_emit(out_key, out_value, output)

if __name__ == '__main__':
    reduce_word_count()
