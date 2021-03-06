#!/usr/bin/env python

import sys
import json
import argparse
import map_reduce_utils as mru

# this should become an arg to map_claims
INPUT_KV_DELIM = '"~~'


def map_claims(input=sys.stdin, output=sys.stdout,
               kv_delim=INPUT_KV_DELIM, stop_words_file=None, stem=True):
    for line in input:
        key, value = line.strip().split(kv_delim)
        patent_id = key.strip()
        if stop_words_file is not None:
            stop_words = json.loads(open(stop_words_file).read())
            contents = mru.clean_text(value, stop_words, stem)
        else:
            contents = mru.clean_text(value, stem=stem)
        key = {'filename': patent_id}
        contents = {'words': [word for word in contents]}
        mru.reducer_emit(key, contents, output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stop-words', dest='stop_words_file')
    parser.add_argument('-t', '--stem', dest='stem', type=bool, default=True)
    args = parser.parse_args()
    stop_words_file = args.stop_words_file
    stem = args.stem
    if stop_words_file is not None:
        map_claims(stop_words_file=stop_words_file, stem=stem)
    else:
        map_claims(stem=stem)
