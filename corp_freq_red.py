#!/usr/bin/env python

from map_reduce_utils import reducer_stream


KEYS = ['word']
VALUES = ['filename', 'freq', 'size', 'count']


def reduce_corpus_frequency(input=reducer_stream(KEYS, VALUES)):
    """
    (word) (file_name n N 1) --> (word file_name) (n N m)

    sums up the number of occurences of each unique word throughout
    the corpus and emits this sum for each document that the word
    occurs in.
    """
    for key, key_stream in input:
        count = 0
        values = []
        for value in key_stream:
            count += int(value['count'])
            values.append(value)
            print_results(values, key['word'], count)


def print_results(values, word, count):
    template = '{0} {1}\t{2} {3} {4}'
    for value in values:
        print template.format(word,
                              value['filename'],
                              value['freq'],
                              value['size'], count)


if __name__ == '__main__':
    reduce_corpus_frequency()
