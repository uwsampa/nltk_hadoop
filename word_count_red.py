#!/usr/bin/env python

import map_reduce_utils as mr_util

"""
(file_name) (word n) --> (word file_name) (n, N)

sums up the total number of words in each document and emits
that sum for each word along with the number of occurences of that
word in the given document
"""

key_names = ['filename']
value_names = ['word', 'frequency']


def print_results(values, filename, count):
    template = '{0} {1}\t{2} {3}'
    for value in values:
        print template.format(value['word'], filename, value['frequency'], count)


for key, key_stream in mr_util.reducer_stream(key_names, value_names):
    count = 0
    values = []
    for value in key_stream:
        values.append(value)
        count += int(value['frequency'])
    print_results(values, key['filename'], count)
