#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def map_word_count(input=sys.stdin, output=sys.stdout):
    """
    (word filename) (n) --> (filename) (word n)

    for each word in each document, emits the document name as the key
    and the word and the number of occurrences in that file as the value
    """

    for in_key, in_value in mru.json_loader(input):
        filename = in_key['filename']
        word = in_key['word']
        word_frequency = in_value['word_freq']
        out_key = {'filename': filename}
        out_value = {'word': word, 'word_freq': word_frequency}
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_word_count()
