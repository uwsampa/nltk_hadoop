#!/usr/bin/env python

import sys
import map_reduce_utils as mru


def reduce_corpus_size(input=sys.stdin, output=sys.stdout):
    for in_key, key_stream in input:
        patents = []
        for in_value in key_stream:
            patents.append((in_key['old_key'], in_value['old_value']))
        for patent_key, patent_value in patents:
            out_key = {'filename': patent_key['filename'],
                       'corpus-size': len(patents)}
            out_value = patent_value
            mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
    reduce_corpus_size()
