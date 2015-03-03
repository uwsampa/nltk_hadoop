#!/usr/bin/env python

from __future__ import print_function
import sys
import map_reduce_utils as mru


def reduce_word_join(input=mru.reducer_stream(), output=sys.stdout):
    """
    (word) (file_name tfidf) --> (word) (file1 file2 tfidf1*tfidf2)

    for each word, if two distinct documents both contain that word,
    a line is emitted containing the product of the tfidf scores of that
    word in both documents.

    This is the first step in computing the pairwise dot product of the tf-idf
    vectors between all documents, where the corresponding elements for every
    pair of documents are multiplied together.
    """

    for in_key, key_stream in input:
        values = []
        for in_value in key_stream:
            values.append(in_value)
        for val1 in in_value:
            for val2 in in_value:
                if not val1['filename'] == val2['filename']:
                    out_key = {'word': in_key['word']}
                    out_value = {'file1': val1['filename'],
                                 'file2': val2['filename'],
                                 'product': val1['tfidf'] * val2['tfidf']}
                    mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
    reduce_word_join()
