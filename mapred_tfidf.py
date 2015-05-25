#!/usr/bin/env python

from __future__ import print_function
import os
import argparse
import sys
import re
import subprocess
import json
import map_reduce_utils as mru


"""
The main runnable script to produce tfidf scores and cosine
similarities for a set of documents. run with '--help' to
see help and arguments.
"""

# the directory where hadoop will read/write to
WORK_DIR_PREFIX = 'hdfs:///patents/output'


def get_output_dir(sub_dir=''):
    """
    prepends the word directory prefix on the names of output directories
    """
    return WORK_DIR_PREFIX + '/' + sub_dir

if __name__ == '__main__':
    # directories where we will store intermediate results
    word_join_dir = get_output_dir('joined_words')
    corpus_size_dir = get_output_dir('corpus_size')
    tfidf_dir = get_output_dir('tfidf')
    normalized_tfidf_dir = get_output_dir('tfidf_normalized')
    corpus_frequency_dir = get_output_dir('corpus_freq')
    word_count_dir = get_output_dir('word_count')
    word_frequency_dir = get_output_dir('word_freq')
    clean_content_dir = get_output_dir('file_contents')

    directories = [clean_content_dir, corpus_size_dir,
                   word_frequency_dir, word_count_dir,
                   corpus_frequency_dir, tfidf_dir,
                   word_join_dir, normalized_tfidf_dir]

    desc = ''' computes the tf-idf cosine simiarity metric for a set
               of documents using map reduce streaming. Set appropriate
               paths in hadoop-streaming-env.sh and 'source' it before
               running this script, or set the corresponding environment
               variables manually.'''

    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    input_help = 'The relative path of the corpus to use as input'
    parser.add_argument('--input', '-i', '--src', default='corpus',
                        dest='input_dir', help=input_help)

    output_help = 'The relative path where the results will be placed'
    parser.add_argument('--output', '-o', '--dst', default='similarities',
                        dest='output_dir', help=output_help)

    force_help = 'If set, silently overwrite output & intermediate dirs: '
    force_help += ' '.join(directories)
    parser.add_argument('--force', '-f', default=False, dest='force',
                        help=force_help, action='store_true')

    n_help = 'n value for n grams'
    parser.add_argument('-n', default=2, dest='n', help=n_help, type=int)

    # default stopwords list is in NLTK
    stop_words_help = 'the list of stop words to filter out. If none, '
    stop_words_help += 'sklearn.feature_extraction.text stop words are used'
    parser.add_argument('-s', '--stop-words', default=None,
                        help=stop_words_help, dest='stop_words')

    args = parser.parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir
    force = args.force
    n = args.n
    stop_words = args.stop_words
    directories.append(output_dir)

    # whether or not we're working in hdfs
    hdfs = map(lambda x: re.match('^hdfs://', x), [input_dir, output_dir])
    hdfs = len([dir for dir in hdfs if dir is not None])

    if hdfs:
        if not force:
            print('The following hdfs dirs will be overwritten:')
            to_delete = directories
            to_delete.append(get_output_dir())
            print('\t', '\n\t'.join(directories))
            response = raw_input('Continue? [y/n] ')
            if response not in ['y', 'yes', 'Y', 'Yes']:
                print('Exiting now')
                exit()
        to_delete = get_output_dir()
        mru.rm_hdfs(to_delete)
        # make a fresh empty dir
        mru.mkdir_hdfs(get_output_dir())
    else:
        # obviously, this won't work if we're using hdfs
        dirs_to_overwrite = filter(os.path.exists, directories)
        if not force and len(dirs_to_overwrite) > 0:
            print('The following directories will be overwritten:')
            print('\t', '\n\t'.join(dirs_to_overwrite))
            response = raw_input('Continue? [y/n] ')
            if response not in ['y', 'yes', 'Y', 'Yes']:
                exit()

    # check to see that environment variables have been set
    env = os.environ.copy()
    try:
        val = env['NLTK_HOME']
    except KeyError as e:
        err_msg = '''
                  ERROR: environment variable NLTK_HOME undefined
                  have you run "source settings.sh"?
                  '''
        print(err_msg, file=sys.stderr)
        raise e

    # do an MR job to clean/stem file contents
    # contents_mapper_cmd = 'contents_mapper.py'
    contents_mapper_cmd = 'claims_mapper.py'
    if stop_words is not None:
        contents_mapper_cmd += ' -s {}'.format(stop_words)
        # need to tell yarn to send stop words file using -files
        mru.run_map_job(contents_mapper_cmd, input_dir, clean_content_dir,
                        files=stop_words, output_format=mru.AVRO_OUTPUT_FORMAT)
    else:
        mru.run_map_job(contents_mapper_cmd, input_dir, clean_content_dir,
                        output_format=mru.AVRO_OUTPUT_FORMAT)


    # calculate corpus size
    # (The output here is a single number, since bringing through all of
    # the claims led to too much mem usage) so we could run this concurrently
    # with the next few jobs
    mru.run_map_reduce_job('corpus_size_map.py', 'corpus_size_red.py',
                           clean_content_dir, corpus_size_dir,
                           input_format=mru.AVRO_INPUT_FORMAT,
                           output_format=mru.AVRO_OUTPUT_FORMAT)
    # Now, parse the result to use later
    corpus_size_location = corpus_size_dir + '/part-00000.avro'
    corpus_size_cmd = 'hadoop fs -cat {}'.format(corpus_size_location)
    corpus_size_output = subprocess.check_output(corpus_size_cmd,
                                                 env=os.environ.copy(),
                                                 shell=True)
    # For now, don't try to parse output as avro. Just splice off the json.
    # Yes, this is . . . bad. Fix it later.
    corpus_size_json = corpus_size_output.split('{')[1].split('}')[0]
    corpus_size_json = '{' + corpus_size_json + '}'
    corpus_size = json.loads(corpus_size_json)['value']


    # calcualte word frequency
    word_freq_map_cmd = 'word_freq_map.py -n {}'.format(n),
    mru.run_map_reduce_job(word_freq_map_cmd, 'word_freq_red.py',
                           clean_content_dir, word_frequency_dir,
                           input_format=mru.AVRO_INPUT_FORMAT,
                           output_format=mru.AVRO_OUTPUT_FORMAT)

    # caclulate word count for each document
    mru.run_map_reduce_job('word_count_map.py', 'word_count_red.py',
                           word_frequency_dir, word_count_dir,
                           input_format=mru.AVRO_INPUT_FORMAT,
                           output_format=mru.AVRO_OUTPUT_FORMAT)

    # calculate word frequency in corpus
    mru.run_map_reduce_job('corp_freq_map.py', 'corp_freq_red.py',
                           word_count_dir, corpus_frequency_dir,
                           input_format=mru.AVRO_INPUT_FORMAT,
                           output_format=mru.AVRO_OUTPUT_FORMAT)

    # now, calculate tfidf scores
    tfidf_command_template = 'tf_idf_map.py -s {}'.format(corpus_size)
    mru.run_map_job(tfidf_command_template,
                    corpus_frequency_dir, tfidf_dir,
                    input_format=mru.AVRO_INPUT_FORMAT,
                    output_format=mru.AVRO_OUTPUT_FORMAT)

    mru.run_map_reduce_job('normalize_mapper.py', 'normalize_reducer.py',
                           tfidf_dir, normalized_tfidf_dir,
                           input_format=mru.AVRO_INPUT_FORMAT,
                           output_format=mru.AVRO_OUTPUT_FORMAT)


    # we're not using hadoop for this dot product any more
    # # join on words for cosine similarity
    # mru.run_map_reduce_job('word_join_map.py', 'word_join_red.py',
    #                        tfidf_dir, word_join_dir,
    #                        input_format=mru.AVRO_INPUT_FORMAT,
    #                        output_format=mru.AVRO_OUTPUT_FORMAT)

    # # now, sum up the products to get the cosine similarities
    # mru.run_map_reduce_job('cos_sim_map.py', 'cos_sim_red.py',
    #                        word_join_dir, output_dir,
    #                        input_format=mru.AVRO_INPUT_FORMAT,
    #                        output_format=mru.AVRO_OUTPUT_FORMAT)
