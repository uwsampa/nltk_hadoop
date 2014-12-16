#!/usr/bin/env python

from map_reduce_utils import reducer_stream

KEYS = ['filename']
VALUES = ['word', 'frequency']


def reduce_word_count(input=reducer_stream(KEYS, VALUES)):
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
        print_results(values, key['filename'], count)


def print_results(values, filename, count):
    template = '{0} {1}\t{2} {3}'
    for value in values:
        print template.format(value['word'], filename,
                              value['frequency'], count)


if __name__ == '__main__':
    reduce_word_count()
