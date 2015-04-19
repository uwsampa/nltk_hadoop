#!/usr/bin/env python

import sys
import map_reduce_utils as mru

# this should become an arg to map_claims
INPUT_KV_DELIMITER = '~~'

def map_claims(input=sys.stdin, output=sys.stdout, kv_delim=INPUT_KV_DELIMITER):
    for line in input:
        key, value = line.strip().split(kv_delim)
        filename = key.strip()
        contents = mru.clean_text(value);
        key = {'filename': filename}
        contents = {'words': [word for word in contents]}
        mru.reducer_emit(key, value, output)


if __name__ == '__main__':
    map_claims()
