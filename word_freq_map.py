#!/usr/bin/env python

import sys


def map_word_frequency(input=sys.stdin):
    """
    (file_name) (file_contents) --> (word file_name) (1)

    maps file contents to words for use in a word count reducer. For each
    word in the document, a new key-value pair is emitted with a value of 1.

    """

    for line in input:
        file_name, words = line.strip().split('\t')
        for word in words.strip().split():
            print '%s %s\t%s' % (word, file_name, 1)


if __name__ == '__main__':
    map_word_frequency()
