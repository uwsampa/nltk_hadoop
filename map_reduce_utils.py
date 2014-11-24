#!/usr/bin/env python

from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stopwords
import string
import re

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
