#!/usr/bin/env python

import sys
import map_reduce_utils as mru


def map_corpus_size(input=sys.stdin, output=sys.stdout):
    for in_key, in_value in mru.json_loader(input):
        out_key = {'count': 1}
        out_value = {'old_key': in_key, 'old_value': in_value}
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    map_corpus_size()
