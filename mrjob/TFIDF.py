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
stop.update(list( "~:;,.()" ))

# this string separates claims in the dataset Jevin gave us.
claim_delimiter = re.compile("~~[0-9]+\. ")

class TFIDF(MRJob):

    # read initial input as tab-delimited key/value, and
    # use JSON for intermediate and final output.
    INPUT_PROTOCOL = RawProtocol
    # INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol  # (default)
    # OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol    # (default)

    # We need to know the total number of patents to compute IDF.
    def configure_options(self):
        super(TFIDF, self).configure_options()
        self.add_passthrough_option(
            '--num-patents', type="int", default=None, help="Specify total number of patents in corpus.")

    # Extract bigrams from patent claims
    def tf_idf_mapper(self, patent_id, line):
        # self.increment_counter('patent', 'count')

        # split patent into claims (for future use)
        claims = re.split(claim_delimiter, line)
        
        # count bigrams in this patent's claims
        bg_counts = collections.Counter()
        for claim in claims:
            tokens = nltk.word_tokenize(claim)
            
            stemmed = [stemmer.stem(t) for t in tokens if t not in stop]
            #stemmed = [t for t in tokens if t not in stop]
            #stemmed = [t for t in tokens]
            
            bgs = bigrams(stemmed)
            
            bg_counts.update(bgs)

        for bg in bg_counts:
            yield bg, (patent_id, 1 + log10(bg_counts[bg]))

    def tf_idf_reducer(self, bg, values):
        vs = list(values)

        idf = log10( float( self.options.num_patents ) / len(vs) )

        for patent_id, tf in vs:
            yield patent_id, (bg, tf * idf)

    def normalizing_reducer(self, patent_id, values):
        v1, v2 = itertools.tee(values)

        length = sqrt( sum( [tf*tf for _, tf in v1] ) )
        
        yield patent_id, [(bg, tf / length) for bg, tf in v2]

    def steps(self):
        return [
            MRStep( mapper   = self.tf_idf_mapper,
                    reducer  = self.tf_idf_reducer ),
            MRStep( reducer  = self.normalizing_reducer ),
            ]


if __name__ == '__main__':
    TFIDF.run()
