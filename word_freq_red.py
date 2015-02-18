#!/usr/bin/env python

from __future__ import print_function
from sys import stdout
import map_reduce_utils as mru


KEYS = ['word', 'filename']
VALUES = ['count']


def reduce_word_frequency(input=mru.reducer_stream(KEYS, VALUES), output=stdout):
    """
    (word file_name) (1) --> (word file_name) (n)

    sums up the number of occurences of each word in each file and emits
    the result for each word/filename combination
    """

    for key, key_stream in input:
        count = 0
        for value in key_stream:
            count += int(value['count'])
        print_result(key, count, output)


def print_result(key, count, output):
    result = '{0} {1}\t{2}'.format(key['word'], key['filename'], count)
    print(result, file=output)


if __name__ == '__main__':
    reduce_word_frequency()
