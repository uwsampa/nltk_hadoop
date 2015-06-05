# note: this is way to slow to use.

from mrjob.job import MRJob
from mrjob.step import MRStep

from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
from nltk.stem import LancasterStemmer

from nltk import bigrams
from nltk.corpus import stopwords

from math import log10
from math import sqrt

import sys
import string
import re
import collections
import itertools


#stemmer = SnowballStemmer("english", ignore_stopwords=True)
#stemmer = PorterStemmer()
stemmer = LancasterStemmer()

stop = set( stopwords.words('english') )
stop.update(list( ":;,.()" ))

claim_delimiter = re.compile("~~[0-9]+\. ")

class CosineSimilarity(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    # INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol  # (default)
    # OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol    # (default)

    def dot_product_mapper(self, patent_id, values):
        for bg, tf in values:
            yield bg, (patent_id, tf)

    def dot_product_reducer(self, bg, values):
        vs = list(values)

        for patent_id_a, tf_a in vs:
            for patent_id_b, tf_b in vs:
                product = tf_a * tf_b
                yield (patent_id_a, patent_id_b), product

    def cosine_similarity_reducer(self, patent_ids, values):
        similarity =  sum(values)
        yield patent_ids, similarity

    def steps(self):
        return [
            MRStep( mapper   = self.dot_product_mapper,
                    reducer  = self.dot_product_reducer ),
            MRStep( reducer  = self.cosine_similarity_reducer ),
            ]


if __name__ == '__main__':
    CosineSimilarity.run()
