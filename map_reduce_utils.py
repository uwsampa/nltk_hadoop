from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stopwords
import string


def clean_text(text):
    # TODO remove words w/ numerals, e.g. '14th'
    stemmer = PorterStemmer()
    result = text.lower()
    result = result.translate(None, string.punctuation)
    result = result.replace('\n', ' ')
    result = result.split()
    result = [stemmer.stem(word) for word in result]
    return filter(lambda word: word not in stopwords, result)
