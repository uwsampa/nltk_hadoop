#!/usr/bin/env python

from __future__ import print_function
import sys


def map_corpus_frequency(input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (n N) --> (word) (file_name n N 1)

    emits a line for each unique word in each file to be consumed
    by corp_freq_red to find the number of occurences of each
    unique word throughout the entire corpus.
    """
    for line in input:
        key, value = line.strip().split('\t')
        word, docname = key.strip().split()
        result = '{0}\t{1} {2} {3}'.format(word, docname, value, 1)
        print(result, file=output)


if __name__ == '__main__':
    map_corpus_frequency()
