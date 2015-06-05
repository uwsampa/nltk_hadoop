#!/usr/bin/env python
# Convert TF-IDF JSON representation into int -> bigram map.
#
# Run with command like:
#   PREFIX=hdfs:///user/nelson/patents-run-1430294014 ; time python BigramMap.py --runner=hadoop --no-output $CLEANUP --setup='source /shared/patents/settings.sh' $PREFIX/tfidf --output-dir=$PREFIX/bmap1

from mrjob.job import MRJob
from mrjob.step import MRStep

from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

from itertools import izip, count

class BigramMap(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    SORT_VALUES    = True

    def mapper(self, patent_id, values):
        for bigram, tf in values:
            yield bigram, None

    def combiner(self, bigram, _):
        yield bigram, None

    def reducer(self, bigram, _):
        yield 0, bigram

    def index_reducer(self, _, bigrams):
        for index, bigram in izip( count(), bigrams ):
            yield index, bigram

    def steps(self):
        return [
            MRStep( mapper   = self.mapper,
                    combiner = self.combiner,
                    reducer  = self.reducer ),
            MRStep( reducer  = self.index_reducer ),
            ]
            
if __name__ == '__main__':
    BigramMap.run()
