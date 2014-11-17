#!/usr/bin/env python

import sys


def print_results(count, files):
    # This is only printing the first element in files, but the output
    # has the same word in two different lines, so it must be the case
    # that this is getting
    for string in files:
        # i think we're not putting a tab in the correct place
        print '%s %s' % (string, count)

processed_files = []
cur_word = None
cur_count = 0

word = None


for line in sys.stdin:
    # do same thing as in step A, but keep a list of all documents that
    # are processed and their n, N values, then when we write out
    # to stdout, we can do it for each one of these
    key, value = line.strip().split('\t')
    word = key.strip()
    docname, word_count, doc_count, count = value.strip().split()
    count = int(count)
    # add document/word combo to processed files
    processed_combo = '%s %s\t%s %s' % (word, docname, word_count, doc_count)
    count = int(count)
    if cur_word == word:
        cur_count += count
        processed_files.append(processed_combo)
    else:
        if cur_word:
            # This is getting hit on the first time a word is seen,
            # we want it on the last one, so need to check before
            # adding counts, print there
            print_results(cur_count, processed_files)
        cur_word = word
        cur_count = count
        processed_files = []
        processed_files.append(processed_combo)

if cur_word == word and cur_word is not None:
    print_results(cur_count, processed_files)
