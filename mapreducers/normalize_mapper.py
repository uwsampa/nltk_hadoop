#!/usr/bin/env python

import sys
import map_reduce_utils as mru


def normalize_mapper(input=sys.stdin, output=sys.stdout):
    for in_key, in_value in mru.json_loader(input):
        ngram = in_key['word']
        uid = in_key['filename']
        out_key = {'uid': uid}
        in_value['ngram'] = ngram
        out_value = in_value
        mru.mapper_emit(out_key, out_value, output)

if __name__ == '__main__':
    normalize_mapper()
