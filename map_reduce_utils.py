#!/usr/bin/env python

from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stopwords
import string
import re
import sys

"""
map_reduce_utils contains helper functions that are used in multiple
map-reduce tasks.
"""


def clean_text(text):
    """
    returns a 'cleaned' version of text by filtering out all words
    that don't contain strictly alphabetic characters, converting
    all words to lowercase, filtering out common stopwords, and
    stemming each word using porter stemming.
    """
    stemmer = PorterStemmer()
    result = text.lower()
    result = result.translate(None, string.punctuation)
    result = result.replace('\n', ' ')
    result = result.split()

    # filter out 'numeric' words such as '14th'
    is_alpha = re.compile('^[a-z]+$')
    result = filter(lambda word: is_alpha.match(word), result)

    result = [stemmer.stem(word) for word in result]
    return filter(lambda word: word not in stopwords, result)


def tokenize_key_value_pair(kv_pair):
    """
    returns a tuple containing the key/value in kv_pair. The key
    is a tuple containing everything before the first tab in kv_pair,
    split on whitespace. The value is a tuple containing everything
    after the first tab in kv_pair, split on whitespace.
    """
    key, value = kv_pair.strip().split('\t')
    key = tuple(key.strip().split())
    value = tuple(value.strip().split())
    return (key, value)


class KeyValueToDict:
    """
    stores the expected state of key-value tuples returned by a tokenizer
    and allows for key-value pairs to subsequently be converted from nested
    tuples to nested dictionaries for easier use. The lists provided to the
    constructor are used as the keys of the dictionaries that are returned.
    """
    def __init__(self, keys, values):
        """
        creates new KeyValueToDict where dictionaries returned by to_dict
        will use the elements in keys to index each element in the key
        and the elements in values to index each element in the value
        """
        self.keys = keys
        self.values = values

    def to_dict(self, kv_pair):
        key = {}
        value = {}
        for i in range(len(self.keys)):
            key[self.keys[i]] = kv_pair[0][i]
        for i in range(len(self.values)):
            value[self.values[i]] = kv_pair[1][i]
        result = {}
        result['key'] = key
        result['value'] = value
        return result


class InputStreamWrapper:
    """
    wraps an input stream function (e.g. sys.stdin.readline) in an
    object so that we can "peek" at the next object emitted from
    the stream without deleting it.
    """

    def __init__(self,
                 source_function=sys.stdin.readline,
                 finished_function=lambda x: len(x()) == 0):
        """
        constructs a new InputStreamWrapperObject which will make calls
        to source_function to retrieve the elements returned by next
        and peek until finished_function returns true
        """
        self.source_function = source_function
        self.finished_function = finished_function
        self.next_element = None

    def peek(self):
        """
        returns the next element in this stream without advancing
        to the next element.
        """
        if self.next_element is None:
            self.next_element = self.source_function()
        return self.next_element

    def next(self):
        """
        returns the next element in this stream and advances to the
        next element
        """
        if not self.has_next():
            raise StopIteration()
        if self.next_element is not None:
            result = self.next_element
            self.next_element = None
        else:
            result = self.source_function()
        return result

    def has_next(self):
        """
        returns true iff there are more elements in this stream
        """
        return not self.finished_function(self.peek)


def reducer_stream(key_names, value_names,
                   src=sys.stdin.readline,
                   tokenizer=tokenize_key_value_pair):
    """
    yields a key and a key_stream for each set of lines in src that have
    equal keys. Keys and values are tokenized with tokenizer and then stored
    in dictionaries so that the nth item in the key or value is indexed by the
    nth item in key_names or value_names, respectively.
    """
    kv_converter = KeyValueToDict(key_names, value_names)
    source_stream = InputStreamWrapper(src)
    while source_stream.has_next():
        key = kv_converter.to_dict(tokenizer(source_stream.peek()))['key']
        yield (key, key_stream(source_stream, kv_converter.to_dict, tokenizer))
    raise StopIteration()


def key_stream(src, dict_converter, tokenizer=tokenize_key_value_pair):
    """
    yeilds values converted to dictionaries with dict_converter from
    src while the keys are the same.
    """
    this_streams_key = None
    while src.has_next():
        next_val = src.peek()
        key, value = tokenizer(next_val)
        if this_streams_key is None:
            this_streams_key = key
        if this_streams_key == key:
            yield dict_converter(tokenizer(src.next()))['value']
        else:
            raise StopIteration()
    raise StopIteration()
