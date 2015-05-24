#!/usr/bin/env python

import sys
import json
import argparse
import nltk_hadoop.map_reduce_utils as mru

# this should become an arg to map_claims
INPUT_KV_DELIM = '"~~'


def map_claims(input=sys.stdin, output=sys.stdout,
               kv_delim=INPUT_KV_DELIM, stop_words_file=None):
    for line in input:
        key, value = line.strip().split(kv_delim)
        patent_id = key.strip()
        if stop_words_file is not None:
            stop_words = json.loads(open(stop_words_file).read())
            contents = mru.clean_text(value, stop_words)
        else:
            contents = mru.clean_text(value)
        key = {'filename': patent_id}
        contents = {'words': [word for word in contents]}
        mru.reducer_emit(key, contents, output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stop-words', dest='stop_words_file')
    args = parser.parse_args()
    stop_words_file = args.stop_words_file
    if stop_words_file is not None:
        map_claims(stop_words_file=stop_words_file)
    else:
        map_claims()
