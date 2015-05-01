#!/usr/bin/env python

import sys
import map_reduce_utils as mru


def reduce_corpus_size(input=mru.reducer_stream(), output=sys.stdout):
    corpus_size = 0
    for in_key, key_stream in input:
        for in_value in key_stream:
            corpus_size = 0
    out_key = {['corpus size']}
    out_value = {[corpus_size]}
    mru.reducer_emit(out_key, out_value)


if __name__ == '__main__':
    reduce_corpus_size()
