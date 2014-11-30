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


class InputStreamWrapper:
    """
    wraps an input stream function (e.g. sys.stdin.readline) in an
    object so that we can "peek" at the next object emitted from
    the stream without deleting it.
    """

    def __init__(self,
                 source_function=sys.stdin.readline,
                 finished_function=lambda x: len(x()) != 0):
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
        if not self.has_next():
            raise StopIteration()
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
        return self.finished_function(self.peek)


def reducer_stream(src=sys.stdin.readline,
                   tokenizer=tokenize_key_value_pair):
    """
    yeilds a key_stream for each set of lines in src that have
    equal keys after being tokenized with tokenizer.
    """
    source_stream = InputStreamWrapper(src)
    while source_stream.has_next():
        yield key_stream(source_stream, tokenizer)
    raise StopIteration()


def key_stream(src, tokenizer=tokenize_key_value_pair):
    """
    yeilds key-value pairs from src while the keys are the same.
    """
    this_streams_key = None
    while src.has_next():
        next_val = src.peek()
        key, value = tokenizer(next_val)
        if this_streams_key is None:
            this_streams_key = key
        if this_streams_key == key:
            yield tokenizer(src.next())
        else:
            raise StopIteration()
    raise StopIteration()
