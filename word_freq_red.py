#!/usr/bin/env python

import sys

cur_word = None
cur_file = None
cur_count = 0
word = None

for line in sys.stdin:
    key, value = line.strip().split('\t')
    word, filename = key.strip().split()
    count = int(value)
    if ((cur_word == word) and (filename == cur_file)):
        cur_count += count
    else:
        if cur_word:
            print '%s %s\t%s' % (cur_word, cur_file, cur_count)
        cur_count = count
        cur_word = word
        cur_file = filename

if cur_word == word and cur_word is not None:
    print '%s %s\t%s' % (cur_word, cur_file, cur_count)
