#!/usr/bin/env python

from __future__ import print_function
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stop_words
import string
import re
import os
import sys
import shutil
import subprocess
import json

"""
map_reduce_utils contains helper functions that are used in multiple
map-reduce tasks.
"""

KV_SEPARATOR = '\t'


APACHE_LIB = 'org.apache.hadoop.mapred'

DEFAULT_INPUT_FORMAT = '{}.TextInputFormat'.format(APACHE_LIB)
DEFAULT_OUTPUT_FORMAT = '{}.TextOutputFormat'.format(APACHE_LIB)


AVRO_IO_LIB = 'org.apache.avro.mapred'

AVRO_INPUT_FORMAT = '{}.AvroAsTextInputFormat'.format(AVRO_IO_LIB)
AVRO_OUTPUT_FORMAT = '{}.AvroTextOutputFormat'.format(AVRO_IO_LIB)

# AVRO_INPUT_FORMAT = 'org.apache.avro.mapreduce.AvroKeyValueInputFormat'
# AVRO_OUTPUT_FORMAT = 'org.apache.avro.mapreduce.AvroKeyValueOutputFormat'


class MapReduceError(Exception):
    """ error raised when a map reduce job fails"""

    def __init__(self, value, source):
        self.value = value
        self.source = source

    def __str__(self):
        return repr(self.value)


def rm_hdfs(dir):
    command = 'hdfs dfs -rm -r {}'.format(dir)
    subprocess.check_call(command, env=os.environ.copy(), shell=True)


def mkdir_hdfs(dir):
    command = 'hdfs dfs -mkdir {}'.format(dir)
    subprocess.check_call(command, env=os.environ.copy(), shell=True)


def run_map_job(mapper, input_dir, output_dir, files='',
                input_format=DEFAULT_INPUT_FORMAT,
                output_format=DEFAULT_OUTPUT_FORMAT,
                kv_separator=KV_SEPARATOR):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = '$NLTK_HOME/' + mapper.strip().split()[0]
    if not output_dir[0:7] == 'hdfs://' and os.path.exists('./' + output_dir):
        shutil.rmtree('./' + output_dir)
    files += map_file
    files += ",$NLTK_HOME/invoke.sh"
    command = '''
      yarn jar $HADOOP_JAR \
         -files {0} \
         -libjars {1} \
         -D mapreduce.job.reduces=0 \
         -D stream.map.output.field.separator={2} \
         -input {3} \
         -output {4} \
         -mapper "$NLTK_HOME/invoke.sh $NLTK_HOME/{5}" \
         -inputformat {6} \
         -outputformat {7}
    '''.format(files, "$AVRO_JAR,$HADOOP_JAR",
               kv_separator, input_dir, output_dir, mapper,
               input_format, output_format).strip()
    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        raise MapReduceError('Map job {0} failed'.format(mapper), e)


def run_map_reduce_job(mapper, reducer, input_dir, output_dir, files='',
                       input_format=DEFAULT_INPUT_FORMAT,
                       output_format=DEFAULT_OUTPUT_FORMAT,
                       kv_separator=KV_SEPARATOR):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = '$NLTK_HOME/' + mapper.strip().split()[0]
    red_file = '$NLTK_HOME/' + reducer.strip().split()[0]
    if not output_dir[0:7] == 'hdfs://' and os.path.exists('./' + output_dir):
        shutil.rmtree('./' + output_dir)

    # all of the additional files each node needs, comma separated
    files += map_file + ',' + red_file + ',$NLTK_HOME/invoke.sh'
    command = '''
      yarn jar $HADOOP_JAR \
         -files {0} \
         -libjars {1} \
         -D stream.map.output.field.separator={2} \
         -mapper "$NLTK_HOME/invoke.sh $NLTK_HOME/{3}" \
         -reducer "$NLTK_HOME/invoke.sh $NLTK_HOME/{4}" \
         -input {5} \
         -output {6} \
         -inputformat {7} \
         -outputformat {8}
    '''.format(files, "$AVRO_JAR,$HADOOP_JAR", kv_separator, mapper, reducer,
               input_dir, output_dir, input_format, output_format)
    command = command.strip()
    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        err_msg = 'ERROR: Map-Reduce job {0}, {1} failed'
        raise MapReduceError(err_msg.format(mapper, reducer), e)


def clean_text(text, stop_word_list=stop_words):
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
    return filter(lambda word: word not in stop_word_list, result)


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


def tokenize_reducer_json(kv_pair):
    kv_pair = json.loads(kv_pair)
    key = kv_pair['key']
    value = kv_pair['value']
    return (key, value)


def tokenize_mapper_json(kv_pair, kv_separator=KV_SEPARATOR):
    # fairly certain this is always a tab even when we change what the
    # separator the mapper emits
    key, value = kv_pair.strip().split(kv_separator)
    key = json.loads(key)
    value = json.loads(value)
    return (key, value)


def reducer_emit(key, value, output=sys.stdout):
    print(json.dumps({'key': key, 'value': value}), file=output)


def mapper_emit(key, value, output=sys.stdout, kv_separator=KV_SEPARATOR):
    key_value = ''.join([json.dumps(key), kv_separator,
                         json.dumps(value)])
    print(key_value, file=output)


class KeyValueToDict:
    """
    stores the expected state of key-value tuples returned by a tokenizer
    and allows for key-value pairs to subsequently be converted from nested
    tuples to nested dictionaries for easier use. The lists provided to the
    constructor are used as the keys of the dictionaries that are returned.
    """
    def __init__(self, keys, values):
        """
        creates new KeyValueToDict where dictionaries returned by to_dict
        will use the elements in keys to index each element in the key
        and the elements in values to index each element in the value
        """
        self.keys = keys
        self.values = values

    def to_dict(self, kv_pair):
        key = {}
        value = {}
        for i in range(len(self.keys)):
            key[self.keys[i]] = kv_pair[0][i]
        for i in range(len(self.values)):
            value[self.values[i]] = kv_pair[1][i]
        result = {}
        result['key'] = key
        result['value'] = value
        return result


class InputStreamWrapper:
    """
    wraps an input stream function (e.g. sys.stdin.readline) in an
    object so that we can "peek" at the next object emitted from
    the stream without deleting it.
    """

    def __init__(self,
                 source_function=sys.stdin.readline,
                 finished_function=lambda x: len(x()) == 0):
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
        return not self.finished_function(self.peek)


def reducer_stream(src=sys.stdin.readline, tokenizer=tokenize_mapper_json):
    """
    yields a key and a key_stream for each set of lines in src that have
    equal keys. Keys and values are tokenized with tokenizer and then stored
    in dictionaries so that the nth item in the key or value is indexed by the
    nth item in key_names or value_names, respectively.
    """
    source_stream = InputStreamWrapper(src)
    while source_stream.has_next():
        key = tokenizer(source_stream.peek())[0]
        yield (key, key_stream(source_stream, tokenizer))
    raise StopIteration()


def key_stream(src, tokenizer=tokenize_mapper_json):
    """
    yeilds values converted to dictionaries with dict_converter from
    src while the keys are the same.
    """
    this_streams_key = None
    while src.has_next():
        next_val = src.peek()
        key, value = tokenizer(next_val)
        if this_streams_key is None:
            this_streams_key = key
        if this_streams_key == key:
            yield tokenizer(src.next())[1]
        else:
            raise StopIteration()
    raise StopIteration()


def json_loader(input=sys.stdin, tokenizer=tokenize_reducer_json):
    for line in input:
        yield tokenizer(line)
    raise StopIteration()
