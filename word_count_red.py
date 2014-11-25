#!/usr/bin/env python

import sys

"""
(file_name) (word n) --> (word file_name) (n, N)

sums up the total number of words in each document and emits
that sum for each word along with the number of occurences of that
word in the given document
"""


def print_results(words, cur_file, cur_count):
    for word, count in words:
        print '{0} {1}\t{2} {3}'.format(word, cur_file, count, cur_count)

words = []
cur_file = None
docname = None
cur_count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    docname = key.strip()
    word, count = value.strip().split()
    count = int(count)

    if docname == cur_file:
        cur_count += count
        words.append((word, count))
    else:
        if cur_file is not None:
            print_results(words, cur_file, cur_count)
        words = []
        cur_file = docname
        cur_count = 0

if cur_file is not None:
    print_results(words, cur_file, cur_count)
