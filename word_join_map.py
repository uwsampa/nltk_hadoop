#!/usr/bin/env python

import sys


def map_word_join(input=sys.stdin):
    """
    (word file_name) (tfidf) --> (word) (file_name tfidf)

    emits a line for each word in each file with the word as a key
    and the filename and tfidf score as the value
    """

    for line in input:
        key, value = line.strip().split('\t')
        word, doc = key.strip().split()
        print '%s\t%s %s' % (word, doc, value)


if __name__ == '__main__':
    map_word_join()
