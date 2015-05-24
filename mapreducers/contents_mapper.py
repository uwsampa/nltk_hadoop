#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import argparse
import map_reduce_utils as mru

"""
(file_contents) --> (file_name) (file_contents)

for each line from stdin consisting of a document in the corpus, emits
a key-value pair to stdout with a key of the corresponding filename
and a value of the file contents cleaned with
map_reduce_utils.clean_text
"""


def map_contents(input=sys.stdin, output=sys.stdout, stop_words=None):
    for line in input:
        docname = os.environ['mapreduce_map_input_file']
        if stop_words is None:
            contents = mru.clean_text(line)
        else:
            contents = mru.clean_text(line, stop_words)
        key = {'filename': docname}
        value = {'words': [word for word in contents]}
        # we emit as if we were a reducer since the contents don't get put
        # through a reducer
        mru.reducer_emit(key, value, output)


def words_in_file(filename):
    results = []
    with open(filename, 'r') as f:
        for line in f:
            words = line.split()
            results += words
    return results


if __name__ == '__main__':
    formatter = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter)
    # default stopwords list is in NLTK
    stop_words_help = 'the list of stop words to filter out. If none, '
    stop_words_help += 'sklearn.feature_extraction.text stop words are used'
    parser.add_argument('-s', '--stop-words', default=None,
                        help=stop_words_help, dest='stop_words')
    args = parser.parse_args()
    if args.stop_words is not None:
        stop_words_list = words_in_file(args.stop_words)
        map_contents(stop_words=stop_words_list)
    else:
        map_contents()
