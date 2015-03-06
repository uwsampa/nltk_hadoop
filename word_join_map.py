#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def map_word_join(input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (tfidf) --> (word) (file_name tfidf)

    emits a line for each word in each file with the word as a key
    and the filename and tfidf score as the value
    """

    for in_key, in_value in mru.json_loader(input):
        out_key = {'word': in_key['word']}
        out_value = {'filename': in_key['filename'], 'tfidf': in_value['tfidf']}
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_word_join()
