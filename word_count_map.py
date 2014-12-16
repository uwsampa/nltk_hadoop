#!/usr/bin/env python

import sys


def map_word_count(input=sys.stdin):
    """
    (word file_name) (n) --> (file_name) (word n)

    for each word in each document, emits the document name as the key
    and the word and the number of occurrences in that file as the value
    """

    for line in input:
        key, value = line.strip().split('\t')
        word, docname = key.strip().split()
        print '%s\t%s %s' % (docname, word, value)


if __name__ == '__main__':
    map_word_count()
