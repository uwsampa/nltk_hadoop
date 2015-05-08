#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru

KEYS_TO_NORMALIZE = ['tfidf', 'log idf', 'idf', 'tf', 'tf log idf']


def normalize_reducer(input=mru.reducer_stream(), output=sys.stdout,
                      keys_to_normalize=KEYS_TO_NORMALIZE):
    for in_key, key_stream in input:
        normalization_factors = {to_factor: 0.0 for to_factor in keys_to_normalize}
        terms_to_normalize = []
        for in_value in key_stream:
            terms_to_normalize.append(in_value)
            normalization_factors = {k: normalization_factors[k] + in_value[k]
                                     for k, v in normalization_factors.iteritems()}
            # normalization_factors = map(lambda x:
            #                             normalization_factors[x] + in_value[x],
            #                             normalization_factors)
        for term in terms_to_normalize:
            out_key = {'uid': in_key['uid'], 'ngram': in_value['ngram']}
            out_value = term
            for key in keys_to_normalize:
                print('normalizing key:', key, file=sys.stderr)
                print('normalization factor:', normalization_factors[key],
                      file=sys.stderr)
                out_value[key] /= normalization_factors[key]
            mru.reducer_emit(out_key, out_value, output)

if __name__ == '__main__':
    normalize_reducer()
