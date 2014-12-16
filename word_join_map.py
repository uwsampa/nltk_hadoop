#!/usr/bin/env python

from __future__ import print_function
import sys


def map_word_join(input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (tfidf) --> (word) (file_name tfidf)

    emits a line for each word in each file with the word as a key
    and the filename and tfidf score as the value
    """

    template = '{}\t{} {}'
    for line in input:
        key, value = line.strip().split('\t')
        word, doc = key.strip().split()
        print(template.format(word, doc, value), file=output)


if __name__ == '__main__':
    map_word_join()
