#!/usr/bin/env python

from __future__ import print_function
import sys


def map_word_frequency(input=sys.stdin, output=sys.stdout):
    """
    (file_name) (file_contents) --> (word file_name) (1)

    maps file contents to words for use in a word count reducer. For each
    word in the document, a new key-value pair is emitted with a value of 1.

    """

    template = '{} {}\t{}'
    for line in input:
        file_name, words = line.strip().split('\t')
        for word in words.strip().split():
            print(template.format(word, file_name, 1), file=output)


if __name__ == '__main__':
    map_word_frequency()
