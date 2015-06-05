#!/usr/bin/env python
# Convert TF-IDF JSON representation into int -> patent_id map.
#
# Run with command like:
#   PREFIX=hdfs:///user/nelson/patents-run-1430294014 ; time python PatentMap.py --runner=hadoop --no-output $CLEANUP --setup='source /shared/patents/settings.sh' $PREFIX/tfidf --output-dir=$PREFIX/pmap1

from mrjob.job import MRJob
from mrjob.step import MRStep

from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

from itertools import izip, count

class PatentMap(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    SORT_VALUES    = True
    
    def mapper(self, patent_id, line):
        yield 0, patent_id

    def reducer(self, _, patent_ids):
        for index, patent_id in izip( count(), patent_ids ):
            yield index, patent_id

if __name__ == '__main__':
    PatentMap.run()
