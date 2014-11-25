#!/usr/bin/env python

import sys

"""
(word) (file_name n N 1) --> (word file_name) (n N m)

sums up the number of occurences of each unique word throughout
the corpus and emits this sum for each document that the word
occurs in.
"""


def print_results(count, files):
    for string in files:
        print '%s %s' % (string, count)

processed_files = []
cur_word = None
cur_count = 0

word = None


for line in sys.stdin:
    key, value = line.strip().split('\t')
    word = key.strip()
    docname, word_count, doc_count, count = value.strip().split()
    count = int(count)
    # add document/word combo to processed files
    processed_combo = '%s %s\t%s %s' % (word, docname, word_count, doc_count)
    if cur_word == word:
        cur_count += count
        processed_files.append(processed_combo)
    else:
        if cur_word is not None:
            print_results(cur_count, processed_files)
        cur_word = word
        cur_count = count
        processed_files = []
        processed_files.append(processed_combo)

if cur_word is not None:
    print_results(cur_count, processed_files)
