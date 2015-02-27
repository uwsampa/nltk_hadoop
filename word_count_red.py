#!/usr/bin/env python

from __future__ import print_function
from map_reduce_utils import reducer_stream
import sys

KEYS = ['filename']
VALUES = ['word', 'frequency']


def reduce_word_count(input=reducer_stream(KEYS, VALUES), output=sys.stdout):
    """
    (file_name) (word n) --> (word file_name) (n, N)

    sums up the total number of words in each document and emits
    that sum for each word along with the number of occurences of that
    word in the given document
    """

    for key, key_stream in input:
        count = 0
        values = []
        for value in key_stream:
            values.append(value)
            count += int(value['frequency'])
        print_results(values, key['filename'], count, output)


def print_results(values, filename, count, output):
    template = '{0} {1}\t{2} {3}'
    for value in values:
        result = template.format(value['word'], filename,
                                 value['frequency'], count)
        print(result, file=output)


if __name__ == '__main__':
    reduce_word_count()
