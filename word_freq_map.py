#!/usr/bin/env python

from __future__ import print_function
import sys
import argparse
import map_reduce_utils as mru


def map_word_frequency(input=sys.stdin, output=sys.stdout, gram_size=1):
    """
    (file_name) (file_contents) --> (word file_name) (1)

    maps file contents to words for use in a word count reducer. For each
    word in the document, a new key-value pair is emitted with a value of 1.
    """

    for in_key, in_value in mru.json_loader(input):
        filename = in_key['filename']
        words = in_value['words']
        out_value = {'count': 1}
        n = gram_size
        if n > len(words):
            n = len(words)
        ngrams = [' '.join(map(lambda x: x, sub_seq)) for sub_seq in
                  [words[i:i + n] for i in range(len(words) - n + 1)]]
        for ngram in ngrams:
            out_key = {'word': ngram, 'filename': filename}
            mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
    formatter = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter)
    nhelp = ' the value of n for n-grams. defaults to 1, i.e. individual words'
    parser.add_argument('-n', default=1, dest='n', help=nhelp, type=int)
    args = parser.parse_args()
    map_word_frequency(gram_size=args.n)
