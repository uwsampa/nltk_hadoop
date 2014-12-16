#!/usr/bin/env python

import sys


def map_corpus_frequency(input=sys.stdin):
    """
    (word file_name) (n N) --> (word) (file_name n N 1)

    emits a line for each unique word in each file to be consumed
    by corp_freq_red to find the number of occurences of each
    unique word throughout the entire corpus.
    """
    for line in input:
        key, value = line.strip().split('\t')
        word, docname = key.strip().split()
        print '{0}\t{1} {2} {3}'.format(word, docname, value, 1)


if __name__ == '__main__':
    map_corpus_frequency()
