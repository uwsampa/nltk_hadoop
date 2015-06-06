#!/usr/bin/env python
#
# Test reading Avro-encoded files
#
# run like
#  python AvroReader.py --runner=hadoop --no-output --setup='source /shared/patents/settings.sh' hdfs:///patents/output/tfidf_normalized/part-00000.avro --hadoop-arg -libjars --hadoop-arg /shared/patents/nltk_hadoop/lib/avro-1.7.7.jar,/shared/patents/nltk_hadoop/lib/avro-mapred-1.7.7.jar --output-dir=avro-test


from mrjob.job import MRJob
from mrjob.step import MRStep

from mrjob.protocol import RawProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol

import sys
import string
import re
import collections
import itertools
import json


class AvroReader(MRJob):

    HADOOP_INPUT_FORMAT = "org.apache.avro.mapred.AvroAsTextInputFormat"
    #HADOOP_OUTPUT_FORMAT = "org.apache.avro.mapred.AvroTextOutputFormat"
    
    # read initial input as tab-delimited key/value, and
    # use JSON for intermediate and final output.
    INPUT_PROTOCOL = JSONValueProtocol
    #INPUT_PROTOCOL = RawValueProtocol
    #INTERNAL_PROTOCOL = RawProtocol
    #OUTPUT_PROTOCOL = RawProtocol

    def mapper(self, _, value):
        yield "argh", value['key']['ngram']


if __name__ == '__main__':
    AvroReader.run()
