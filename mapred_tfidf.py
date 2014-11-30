#!/usr/bin/env python

from __future__ import print_function
import os
import subprocess
import argparse
import shutil
import sys

"""
The main runnable script to produce tfidf scores and cosine
similarities for a set of documents. run with '--help' to
see help and arguments.
"""


class MapReduceError(Exception):
    """ error raised when a map reduce job fails"""

    def __init__(self, value, source):
        self.value = value
        self.source = source

    def __str__(self):
        return repr(self.value)


def run_map_job(mapper, input_dir, output_dir):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = '$NLTK_HOME/' + mapper.strip().split()[0]
    map_file = mapper.strip().split()[0]
    if os.path.exists('./' + output_dir):
        shutil.rmtree('./' + output_dir)
    command = '''
      $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/$RELATIVE_PATH_JAR \
         -D mapred.job.reduces=0 \
         -mapper "$NLTK_HOME/{0}" \
         -input $NLTK_HOME/{1} \
         -output $NLTK_HOME/{2} \
         -file {3}\
    '''.format(mapper, input_dir, output_dir, map_file).strip()

    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        raise MapReduceError('Map job {0} failed'.format(mapper), e)


def run_map_reduce_job(mapper, reducer, input_dir, output_dir):
    env = os.environ.copy()
    # we have to pass the specific files as well to allow for
    # arguments to the mapper and reducer
    map_file = '$NLTK_HOME/' + mapper.strip().split()[0]
    red_file = '$NLTK_HOME/' + mapper.strip().split()[0]
    if os.path.exists('./' + output_dir):
        shutil.rmtree('./' + output_dir)
    command = '''
      $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/$RELATIVE_PATH_JAR \
         -mapper "$NLTK_HOME/{0}" \
         -reducer "$NLTK_HOME/{1}" \
         -input $NLTK_HOME/{2} \
         -output $NLTK_HOME/{3} \
         -file {4} \
         -file {5}
    '''.format(mapper, reducer, input_dir, output_dir, map_file, red_file)
    command = command.strip()
    try:
        subprocess.check_call(command, env=env, shell=True)
    except subprocess.CalledProcessError as e:
        err_msg = 'ERROR: Map-Reduce job {0}, {1} failed'
        raise MapReduceError(err_msg.format(mapper, reducer), e)

if __name__ == '__main__':
    # directories where we will store intermediate results
    word_join_dir = 'joined_words'
    tfidf_dir = 'tfidf'
    corpora_frequency_dir = 'corpora_freq'
    word_count_dir = 'word_count'
    word_frequency_dir = 'word_freq'
    clean_content_dir = 'file_contents'

    directories = [clean_content_dir, word_frequency_dir,
                   word_count_dir, corpora_frequency_dir,
                   tfidf_dir, word_join_dir]

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

    precision_help = 'The number of digits of precision the results will be'
    parser.add_argument('--precision', '-p', default=10, dest='precision',
                        help=precision_help)

    args = parser.parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir
    force = args.force
    precision = args.precision
    directories.append(output_dir)

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
                  have you run "source hadoop-streaming-env.sh"?
                  '''
        print(err_msg, file=sys.stderr)
        raise e

    # we need the size of the corpora to do tfidf:
    corp = './' + input_dir
    corp_files = [f for f in os.listdir(corp) if os.path.isfile(corp+'/'+f)]
    corpora_len = len(corp_files)

    # do an MR job to clean/stem file contents
    run_map_job('contents_mapper.py', input_dir, clean_content_dir)

    # calcualte word frequency
    run_map_reduce_job('word_freq_map.py',
                       'word_freq_red.py',
                       clean_content_dir,
                       word_frequency_dir)

    # caclulate word count for each document
    run_map_reduce_job('word_count_map.py',
                       'word_count_red.py',
                       word_frequency_dir,
                       word_count_dir)

    # calculate word frequency in corpora
    run_map_reduce_job('corp_freq_map.py',
                       'corp_freq_red.py',
                       word_count_dir,
                       corpora_frequency_dir)

    # now, calculate tfidf scores
    run_map_job('tf_idf_map.py -s {0} -p {1}'.format(corpora_len, precision),
                corpora_frequency_dir,
                tfidf_dir)

    # join on words for cosine similarity
    run_map_reduce_job('word_join_map.py',
                       'word_join_red.py -p {0}'.format(precision),
                       tfidf_dir,
                       word_join_dir)

    # now, sum up the products to get the cosine similarities
    run_map_reduce_job('cos_sim_map.py',
                       'cos_sim_red.py -p {0}'.format(precision),
                       word_join_dir,
                       output_dir)
