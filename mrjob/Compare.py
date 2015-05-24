#!/usr/bin/env python

#
# Hadoop job to compare our cosine similarities with those from jevin's database
#
# run with command like:
#    python Compare.py --runner=hadoop --no-output --setup='source /shared/patents/settings.sh' hdfs:///user/nelson/cosine_similarity.tsjson hdfs:///user/nelson/jevin_similarity.tsjson --output-dir=jevin_compare12
#

from mrjob.job import MRJob

from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

class Compare(MRJob):

    INPUT_PROTOCOL = JSONProtocol

    def reducer(self, patent_ids, values):
        vs = list(values)       # convert to list
        if len(vs) > 1:      # if we have values to compare
            if len(vs) > 2:
                print "Warning: found more than two values for patents {}: {}".format(patent_ids, vs)
            else:
                v1, v2 = vs
                yield patent_ids, abs(float(v1) - float(v2))
        
if __name__ == '__main__':
    Compare.run()
