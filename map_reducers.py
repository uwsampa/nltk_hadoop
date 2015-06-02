#!/usr/bin/env python


import sys
import json
import map_reduce_utils as mru
import math

# this should become an arg to map_claims
INPUT_KV_DELIM = '"~~'


def map_claims(input=sys.stdin, output=sys.stdout,
               kv_delim=INPUT_KV_DELIM, stop_words_file=None, stem=True):
    for line in input:
        key, value = line.strip().split(kv_delim)
        patent_id = key.strip()
        if stop_words_file is not None:
            stop_words = json.loads(open(stop_words_file).read())
            contents = mru.clean_text(value, stop_words, stem)
        else:
            contents = mru.clean_text(value, stem=stem)
        key = {'filename': patent_id}
        contents = {'words': [word for word in contents]}
        mru.reducer_emit(key, contents, output)


def map_corpus_size(input=sys.stdin, output=sys.stdout):
    for in_key, in_value in mru.json_loader(input):
        out_key = {'count': 1}
        out_value = {'count': 1}
        mru.mapper_emit(out_key, out_value, output)


def reduce_corpus_size(input=mru.reducer_stream(), output=sys.stdout):
    corpus_size = 0
    for in_key, key_stream in input:
        for in_value in key_stream:
            corpus_size += 1
    out_key = 'corpus size'
    out_value = corpus_size
    mru.reducer_emit(out_key, out_value, output)


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
        ngrams = [' '.join(map(lambda x: x, words[i:i + n]))
                  for i in range(len(words) - n + 1)]
        for ngram in ngrams:
            out_key = {'word': ngram, 'filename': filename}
            mru.mapper_emit(out_key, out_value, output)


def reduce_word_frequency(input=mru.reducer_stream(), output=sys.stdout):
    """
    (word filename) (1) --> (word filename) (n)

    sums up the number of occurences of each word in each file and emits
    the result for each word/filename combination
    """

    for in_key, key_stream in input:
        word_frequency = 0
        for in_value in key_stream:
            word_frequency += in_value['count']
        out_key = {'word': in_key['word'], 'filename': in_key['filename']}
        out_value = {'word_freq': word_frequency}
        mru.reducer_emit(out_key, out_value, output)


def map_word_count(input=sys.stdin, output=sys.stdout):
    """
    (word filename) (n) --> (filename) (word n)

    for each word in each document, emits the document name as the key
    and the word and the number of occurrences in that file as the value
    """

    for in_key, in_value in mru.json_loader(input):
        filename = in_key['filename']
        word = in_key['word']
        word_frequency = in_value['word_freq']
        out_key = {'filename': filename}
        out_value = {'word': word, 'word_freq': word_frequency}
        mru.mapper_emit(out_key, out_value, output)


def reduce_word_count(input=mru.reducer_stream(), output=sys.stdout):
    """
    (file_name) (word word_freq) --> (word file_name) (n N)

    sums up the total number of words in each document and emits
    that sum for each word along with the number of occurences of that
    word in the given document
    """

    for in_key, key_stream in input:
        doc_size = 0
        values = []
        for in_value in key_stream:
            values.append(in_value)
            doc_size += in_value['word_freq']
        for value in values:
            out_key = {'word': value['word'], 'filename': in_key['filename']}
            out_value = {'word_freq': value['word_freq'], 'doc_size': doc_size}
            mru.reducer_emit(out_key, out_value, output)


def map_corpus_frequency(input=sys.stdin, output=sys.stdout):
    """
    (word filename) (n N) --> (word) (filename n N 1)

    emits a line for each unique word in each file to be consumed
    by corp_freq_red to find the number of occurences of each
    unique word throughout the entire corpus.
    """
    for in_key, in_value in mru.json_loader(input):
        out_key = {'word': in_key['word']}
        out_value = {'filename': in_key['filename'],
                     'word_freq': in_value['word_freq'],
                     'doc_size': in_value['doc_size'],
                     'count': 1}
        mru.mapper_emit(out_key, out_value, output)


def reduce_corpus_frequency(input=mru.reducer_stream(), output=sys.stdout):
    """
    (word) (filename n N 1) --> (word filename) (n N m)

    sums up the number of occurences of each unique word throughout
    the corpus and emits this sum for each document that the word
    occurs in.
    """
    for in_key, key_stream in input:
        corpus_frequency = 0
        values = []
        for in_value in key_stream:
            corpus_frequency += in_value['count']
            values.append(in_value)
        for value in values:
            out_key = {'word': in_key['word'], 'filename': value['filename']}
            out_value = {'word_freq': value['word_freq'],
                         'doc_size': value['doc_size'],
                         'corp_freq': corpus_frequency}
            mru.reducer_emit(out_key, out_value, output)


def map_tf_idf(corpus_size, input=sys.stdin, output=sys.stdout):
    """
    (word file_name) (n N m) --> (word file_name) (tfidf)

    computes the tf-idf metric for each word in each file in the corpus
    which is defined as the term frequency multiplied by the inverse document
    frequency. The term frequency is what porportion of the words in
    the document are a given word. The inverse document frequency is the
    number of documents in the corpus that the word appears.
    """

    for in_key, in_value in mru.json_loader(input):
        n = in_value['word_freq']
        N = in_value['doc_size']
        m = in_value['corp_freq']
        D = corpus_size
        tf = float(n) / float(N)
        idf = (float(D) / float(m))
        log_idf = math.log(idf, 10)
        tfidf = tf * idf
        tf_log_idf = tf * log_idf
        # in_key == out_key
        out_value = {'tfidf': tfidf, 'tf log idf': tf_log_idf,
                     'log idf': log_idf, 'idf': idf, 'tf': tf,
                     'word frequency': n, 'document length': N,
                     'corpus frequency': m, 'corpus size': D}
        mru.reducer_emit(in_key, out_value, output)


def normalize_mapper(input=sys.stdin, output=sys.stdout):
    for in_key, in_value in mru.json_loader(input):
        ngram = in_key['word']
        uid = in_key['filename']
        out_key = {'uid': uid}
        in_value['ngram'] = ngram
        out_value = in_value
        mru.mapper_emit(out_key, out_value, output)


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
