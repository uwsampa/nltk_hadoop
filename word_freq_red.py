#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def reduce_word_frequency(input=mru.reducer_stream(), output=sys.stdout):
    """
    (word filename) (1) --> (word filename) (n)

    sums up the number of occurences of each word in each file and emits
    the result for each word/filename combination
    """

    for in_key, key_stream in input:
        word_frequency = 0
        for in_value in key_stream:
            word_frequency += in_value['count']
        out_key = {'word': in_key['word'], 'filename': in_key['filename']}
        out_value = {'word_freq': word_frequency}
        mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
    reduce_word_frequency()
