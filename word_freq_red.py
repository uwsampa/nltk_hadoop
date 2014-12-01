#!/usr/bin/env python

import map_reduce_utils as mr_util

"""
(word file_name) (1) --> (word file_name) (n)

sums up the number of occurences of each word in each file and emits
the result for each word/filename combination
"""

keys = ['word', 'filename']
values = ['count']


def print_result(key, count):
    print '{0} {1}\t{2}'.format(key['word'], key['filename'], count)

for key, key_stream in mr_util.reducer_stream(keys, values):
    count = 0
    for value in key_stream:
        count += int(value['count'])
    print_result(key, count)
