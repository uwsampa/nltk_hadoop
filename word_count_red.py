#!/usr/bin/env python

import map_reduce_utils as mr_util

"""
(file_name) (word n) --> (word file_name) (n, N)

sums up the total number of words in each document and emits
that sum for each word along with the number of occurences of that
word in the given document
"""

keys = ['filename']
values = ['word', 'frequency']
kv_convert = mr_util.KeyValueToDict(keys, values)


def print_results(values, filename, count):
    template = '{0} {1}\t{2} {3}'
    for value in values:
        print template.format(value['word'], filename, value['frequency'], count)


for key_stream in mr_util.reducer_stream():
    count = 0
    values = []
    for kv_pair in key_stream:
        keyval_dict = kv_convert.to_dict(kv_pair)
        values.append(keyval_dict['value'])
        count += int(keyval_dict['value']['frequency'])
    print_results(values, keyval_dict['key']['filename'], count)
