#!/usr/bin/env python

from __future__ import print_function
import sys
import math
import map_reduce_utils as mru

KEYS_TO_NORMALIZE = ['tfidf', 'log idf', 'idf', 'tf', 'tf log idf']


def normalize_reducer(input=mru.reducer_stream(), output=sys.stdout,
                      keys_to_normalize=KEYS_TO_NORMALIZE):
    for in_key, key_stream in input:
        normalize_factors = {to_factor: 0.0 for to_factor in keys_to_normalize}
        terms_to_normalize = []
        for in_value in key_stream:
            terms_to_normalize.append(in_value)
            normalize_factors = {k: normalize_factors[k] + in_value[k] ** 2
                                 for k, v in normalize_factors.iteritems()}
        for term in terms_to_normalize:
            out_key = {'uid': in_key['uid'], 'ngram': term['ngram']}
            out_value = term
            del out_value['ngram']
            for key in keys_to_normalize:
                out_value[key] /= math.sqrt(normalize_factors[key])
            mru.reducer_emit(out_key, out_value, output)

if __name__ == '__main__':
    normalize_reducer()
