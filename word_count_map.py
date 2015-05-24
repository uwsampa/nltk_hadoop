#!/usr/bin/env python

from __future__ import print_function
import sys


def map_word_count(input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (n) --> (file_name) (word n)

    for each word in each document, emits the document name as the key
    and the word and the number of occurrences in that file as the value
    """

    template = '{}\t{} {}'
    for line in input:
        key, value = line.strip().split('\t')
        word, docname = key.strip().split()
        print(template.format(docname, word, value), file=output)


if __name__ == '__main__':
    map_word_count()
