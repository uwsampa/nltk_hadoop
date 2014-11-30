#!/usr/bin/env python

import map_reduce_utils as mr_util

"""
(word) (file_name n N 1) --> (word file_name) (n N m)

sums up the number of occurences of each unique word throughout
the corpus and emits this sum for each document that the word
occurs in.
"""

keys = ['word']
values = ['filename', 'freq', 'size', 'count']
kv_convert = mr_util.KeyValueToDict(keys, values)


def print_results(values, word, count):
    template = '{0} {1}\t{2} {3} {4}'
    for value in values:
        print template.format(word,
                              value['filename'],
                              value['freq'],
                              value['size'], count)


for key_stream in mr_util.reducer_stream():
    count = 0
    values = []
    for kv_pair in key_stream:
        kv_dict = kv_convert.to_dict(kv_pair)
        count += int(kv_dict['value']['count'])
        values.append(kv_dict['value'])
    print_results(values, kv_dict['key']['word'], count)
