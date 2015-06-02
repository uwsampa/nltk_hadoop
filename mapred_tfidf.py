#!/usr/bin/env python

from __future__ import print_function
import os
import argparse
import sys
import re
import map_reducers
import map_reduce_utils as mru
import hadoop_utils as hu


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

    stemmer_help = 'if true, use nltk PorterStemmer to stem ngrams'
    parser.add_argument('--stem', action='store_true',
                        dest='stem', help=stemmer_help)
    parser.add_argument('--no-stem', action='store_false', dest='stem',
                        help=stemmer_help)
    parser.set_defaults(stem=True)

    args = parser.parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir
    force = args.force
    n = args.n
    stop_words = args.stop_words
    stem = args.stem
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
        hu.hdfs_remove_directory(to_delete)
        # make a fresh empty dir
        hu.hdfs_make_directory(get_output_dir())
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
    claims_mapper_fn = map_reducers.map_claims
    claims_mapper_args = {'kv_delim': '"~~', 'stem': stem}
    if stop_words is not None:
        claims_mapper_args['stop_words'] = stop_words

    claims_mapper_result = mru.run_map_job(
        claims_mapper_fn, claims_mapper_args,
        src=input_dir, dst=clean_content_dir,
        files=stop_words, output_format=mru.AVRO_OUTPUT_FORMAT)

    # calculate corpus size
    corpus_size_map_fn = map_reducers.map_corpus_size
    corpus_size_red_fn = map_reducers.reduce_corpus_size

    corpus_size_results = mru.run_map_reduce_job(
        corpus_size_map_fn, corpus_size_red_fn,
        src=claims_mapper_result, dst=corpus_size_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)

    corpus_size_record = corpus_size_results.next()
    corpus_size = corpus_size_record['corpus size']

    # calcualte word frequency
    word_frequency_map_fn = map_reducers.map_word_frequency
    word_frequency_reduce_fn = map_reducers.reducer_word_frequency
    word_frequency_map_args = {'n': n}

    word_frequency_result = mru.run_map_reduce_job(
        word_frequency_map_fn, word_frequency_reduce_fn,
        src=claims_mapper_result, dst=word_frequency_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)

    # caclulate word count for each document
    word_count_map_fn = map_reducers.map_word_count
    word_count_reduce_fn = map_reducers.reduce_word_count
    word_count_results = mru.run_map_reduce_job(
        word_count_map_fn, word_count_reduce_fn,
        src=word_frequency_result, dst=word_count_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)

    # calculate word frequency in corpus
    corpus_frequency_map_fn = map_reducers.map_corpus_frequency
    corpus_frequency_reduce_fn = map_reducers.reduce_corpus_frequency
    corpus_frequency_results = mru.run_map_reduce_job(
        corpus_frequency_map_fn, corpus_frequency_reduce_fn,
        src=word_count_results, dst=corpus_frequency_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)

    # calculate tfidf scores
    tfidf_map_fn = map_reducers.map_tf_idf
    tfidf_map_args = {'corpus_size': corpus_size}
    tfidf_results = mru.run_map_job(
        tfidf_map_fn, tfidf_map_args,
        src=corpus_frequency_results, dst=tfidf_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)


    normalize_map_fn = map_reducers.normalize_mapper
    normalize_reduce_fn = map_reducers.normalize_reducer
    normalized_tfidf_results = mru.run_map_reduce_job(
        normalize_map_fn, normalize_reduce_fn,
        src=tfidf_results, dst=normalized_tfidf_dir,
        input_format=mru.AVRO_INPUT_FORMAT, output_format=mru.AVRO_OUTPUT_FORMAT)
