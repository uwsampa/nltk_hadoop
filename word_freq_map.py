#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def map_word_frequency(input=sys.stdin, output=sys.stdout):
    """
    (file_name) (file_contents) --> (word file_name) (1)

    maps file contents to words for use in a word count reducer. For each
    word in the document, a new key-value pair is emitted with a value of 1.
    """

    for in_key, in_value in mru.json_loader(input):
        filename = in_key['filename']
        words = in_value['words']
        out_value = {'count': 1}
        for word in words:
            out_key = {'word': word, 'filename': filename}
            mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_word_frequency()
