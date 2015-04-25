#!/usr/bin/env python

import sys
import map_reduce_utils as mru

# this should become an arg to map_claims
INPUT_KV_DELIM = '"~~'


def map_claims(input=sys.stdin, output=sys.stdout, kv_delim=INPUT_KV_DELIM):
    for line in input:
        key, value = line.strip().split(kv_delim)
        patent_id = key.strip()
        contents = mru.clean_text(value)
        key = {'filename': patent_id}
        contents = {'words': [word for word in contents]}
        mru.reducer_emit(patent_id, contents, output)


if __name__ == '__main__':
    map_claims()
