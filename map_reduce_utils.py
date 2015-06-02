#!/usr/bin/env python

from __future__ import print_function
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as stop_words
import string
import StringIO
import re
import os
import sys
import shutil
import subprocess
import json
import inspect
import hadoop_utils as hu


"""
map_reduce_utils contains helper functions that are used in multiple
map-reduce tasks.
"""

KV_SEPARATOR = '\t'

DEFAULT_CODEC = 'deflate'

APACHE_LIB = 'org.apache.hadoop.mapred'

DEFAULT_INPUT_FORMAT = '{}.TextInputFormat'.format(APACHE_LIB)
DEFAULT_OUTPUT_FORMAT = '{}.TextOutputFormat'.format(APACHE_LIB)

AVRO_IO_LIB = 'org.apache.avro.mapred'

AVRO_INPUT_FORMAT = '{}.AvroAsTextInputFormat'.format(AVRO_IO_LIB)
AVRO_OUTPUT_FORMAT = '{}.AvroTextOutputFormat'.format(AVRO_IO_LIB)

INVOKE_PYTHON_CMD = '$NLTK_HOME/invoke.sh python -c "import imp;' + \
                    'mapred_module = imp.load_source(\'\', \'{source_file}\');' + \
                    'mapred_module.{function_to_invoke}({args})"'

DEFAULT_MAPREDUCE_ARGS = {}


class MapReduceError(Exception):
    """ error raised when a map reduce job fails"""

    def __init__(self, value, source):
        self.value = value
        self.source = source

    def __str__(self):
        return repr(self.value)


def get_mapreducer_cmd(*args, **kwargs):
    format_args = {}
    for key in ('mapreducer', 'args'):
        if key in kwargs:
            format_args[key] = kwargs[key]
        elif key is 'args':
            format_args[key] = DEFAULT_MAPREDUCE_ARGS
        else:
            raise Exception('mapper or reducer command missing arg:', key)
    format_args['function_to_invoke'] = format_args['mapreducer'].__file__
    format_args['source_file'] = inspect.getabsfile(format_args['mapreducer'])
    format_args['args'] = ', '.join('{}={}'.format(key, args[key]) for key in args)
    cmd = INVOKE_PYTHON_CMD.format(format_args)

    # since we're escaping single quotes in the format skeleton,
    # we need to "print" the string into another string so that
    # those single quotes are no longer escaped
    f = StringIO.StringIO()
    f.write(cmd)
    cmd = f.getvalue()
    return cmd


def run_map_job(mapper, args={}, src='', dst='', files='',
                src_format=DEFAULT_INPUT_FORMAT,
                dst_format=DEFAULT_OUTPUT_FORMAT,
                kv_separator=KV_SEPARATOR, codec=DEFAULT_CODEC):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = inspect.getabsfile(mapper)

    # if we were given a MapReduceResult as input, then use it's hdfs
    # output directory as the input directory for this job
    if isinstance(src, MapReduceResult):
        src = src.directory

    # only use compression if our output format is avro:
    compression_arg = ''
    if dst_format == AVRO_OUTPUT_FORMAT:
        compression_arg = '-D mapreduce.output.fileoutputformat.compress=true'
        compression_arg += ' -D avro.output.codec={}'.format(DEFAULT_CODEC)

    if files == '':
        files = map_file
    else:
        files += ',' + map_file
    files += ",$NLTK_HOME/invoke.sh"

    map_command = get_mapreducer_cmd(mapreducer=mapper, args=args)

    command = '''
      yarn jar $HADOOP_JAR \
         -files {files} \
         -libjars {jars} \
         -D mapreduce.job.reduces=0 \
         -D stream.map.output.field.separator={separator} \
         {compression} \
         -input {input} \
         -output {output} \
         -mapper "{map_cmd}" \
         -inputformat {in_format} \
         -outputformat {out_format} \
    '''.format(files=files, jars="$AVRO_JAR,$HADOOP_JAR",
               separator=kv_separator, input=src, output=dst,
               map_cmd=map_command, compression=compression_arg,
               in_format=src_format, out_format=dst_format).strip()
    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        raise MapReduceError('Map job {0} failed'.format(mapper), e)

    return MapReduceResult(output_dir=dst, output_format=dst_format)


def run_map_reduce_job(mapper, reducer, mapper_args={}, reducer_args={},
                       src='', dst='', files=None,
                       src_format=DEFAULT_INPUT_FORMAT,
                       dst_format=DEFAULT_OUTPUT_FORMAT,
                       kv_separator=KV_SEPARATOR, codec=DEFAULT_CODEC):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = inspect.getabsfile(mapper)
    red_file = inspect.getabsfile(reducer)

    # if we were given a MapReduceResult as input, then use it's hdfs
    # output directory as the input directory for this job
    if isinstance(src, MapReduceResult):
        src = src.directory

    # only use compression if our output format is avro:
    compression_arg = ''
    if dst_format == AVRO_OUTPUT_FORMAT:
        compression_arg = '-D mapreduce.output.fileoutputformat.compress=true'
        compression_arg += ' -D avro.output.codec={}'.format(DEFAULT_CODEC)

    # all of the additional files each node needs, comma separated
    if files is None:
        files = map_file + ',' + red_file + ',$NLTK_HOME/invoke.sh'
    else:
        files += map_file + ',' + red_file + ',$NLTK_HOME/invoke.sh'

    mapper_command = get_mapreducer_cmd(mapreducer=mapper, args=mapper_args)

    reducer_command = get_mapreducer_cmd(mapreduver=reducer, args=reducer_args)

    command = '''
      yarn jar $HADOOP_JAR \
         -files {files} \
         -libjars {jars} \
         -D stream.map.output.field.separator={separator} \
         {compression} \
         -mapper "{mapper}" \
         -reducer "{reducer}" \
         -input {input} \
         -output {output} \
         -inputformat {input_format} \
         -outputformat {output_format}
    '''.format(files=files, jars="$AVRO_JAR,$HADOOP_JAR", separator=kv_separator,
               mapper=mapper_command, reducer=reducer_command,
               input=src, output=dst, input_format=src_format,
               output_format=src_format, compression=compression_arg)
    command = command.strip()
    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        err_msg = 'ERROR: Map-Reduce job {0}, {1} failed'
        raise MapReduceError(err_msg.format(mapper, reducer), e)

    return MapReduceResult(output_dir=dst, output_format=dst_format)


def clean_text(text, stop_word_list=stop_words, stem=True):
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

    if stem:
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


class MapReduceResult(object):
    """
    uses webhdfs to provide the results of a map reduce job in the form of a
    generator. can also be passed to a subsequent map reduce jobs to be used
    as input (don't worry though, this doesn't go through webhdfs).
    """

    def __init__(self, *args, **kwargs):
        """
        takes two kwargs: directory and results. directory is the hdfs location
        where results are stored and results is a generator over the results
        """
        self.directory = re.sub('hdfs://.*/', '/', kwargs['directory'])
        if kwargs['output_format'] is AVRO_OUTPUT_FORMAT:
            self.hdfs_results = hu.hdfs_avro_records(self.directory)
        else:
            self.hdfs_results = hu.hdfs_dir_contents(self.directory)

    def directory(self):
        """
        the hdfs location where results are stored
        """
        return self.directory

    def __iter__(self):
        return self

    def next(self):
        try:
            next_record = self.hdfs_results.next()
            yield json.loads(next_record)
        except ValueError:
            yield next_record
        except StopIteration:
            raise StopIteration()
