#!/usr/bin/env python
#
# Test reading Avro-encoded files
#
# run like
#   python AvroReader.py --runner=hadoop --no-output --setup='source /shared/patents/settings.sh' hdfs:///patents/output/tfidf_normalized/part-00000.avro --output-dir=avro-test1
# but currently fails

from mrjob.job import MRJob
from mrjob.step import MRStep

from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

import sys
import string
import re
import collections
import itertools
import json


class AvroReader(MRJob):

    HADOOP_INPUT_FORMAT = "org.apache.avro.mapred.AvroAsTextInputFormat"
    HADOOP_OUTPUT_FORMAT = "org.apache.avro.mapred.AvroAsTextInputFormat"
    
    # read initial input as tab-delimited key/value, and
    # use JSON for intermediate and final output.
    INPUT_PROTOCOL = JSONProtocol
    INTERNAL_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def mapper(self, key, value):
        yield key, value
        
    def steps(self):
        return [
            MRStep( mapper   = self.mapper ),
            ]


if __name__ == '__main__':
    AvroReader.run()
