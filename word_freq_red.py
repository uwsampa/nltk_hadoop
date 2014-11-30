#!/usr/bin/env python

import map_reduce_utils as mr_util

"""
(word file_name) (1) --> (word file_name) (n)

sums up the number of occurences of each word in each file and emits
the result for each word/filename combination
"""

keys = ['word', 'filename']
values = ['count']
kv_convert = mr_util.KeyValueToDict(keys, values)


def print_result(keyval, count):
    print '{0} {1}\t{2}'.format(keyval['key']['word'],
                                keyval['key']['filename'],
                                count)

for key_stream in mr_util.reducer_stream():
    count = 0
    for kv_pair in key_stream:
        keyval_dict = kv_convert.to_dict(kv_pair)
        count += int(keyval_dict['value']['count'])
    print_result(keyval_dict, count)
