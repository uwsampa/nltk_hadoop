#!/usr/bin/env python

import os
import subprocess
import sys
import shutil


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
    except subprocess.CalledProcessError:
        print 'ERROR: Map job %s failed' % mapper
        raise


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
    except subprocess.CalledProcessError:
        print 'ERROR: Map-Reduce job %s, %s failed' % (mapper, reducer)
        raise

if __name__ == '__main__':
    env = os.environ.copy()
    input_dir = "inaugural"
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
    try:
        val = env['NLTK_HOME']
    except KeyError:
        print 'ERROR: Please run "source ./hadoop-streaming-env.sh"'
        raise

    # we need the size of the corpora to do tfidf:
    corp = './' + input_dir
    corp_files = [f for f in os.listdir(corp) if os.path.isfile(corp+'/'+f)]
    corpora_len = len(corp_files)
    print 'CORP_SIZE: ', corpora_len

    # TODO: probably shouldn't clobber these dirs in case there's
    # anything in them

    # do an MR job to clean/stem file contents
    clean_content_dir = 'file_contents'
    run_map_job('contents_mapper.py', input_dir, clean_content_dir)

    # calcualte word frequency
    word_frequency_dir = 'word_freq'
    run_map_reduce_job('word_freq_map.py',
                       'word_freq_red.py',
                       clean_content_dir,
                       word_frequency_dir)

    # caclulate word count for each document
    word_count_dir = 'word_count'
    run_map_reduce_job('word_count_map.py',
                       'word_count_red.py',
                       word_frequency_dir,
                       word_count_dir)

    # calculate word frequency in corpora
    corpora_frequency_dir = 'corpora_freq'
    run_map_reduce_job('corp_freq_map.py',
                       'corp_freq_red.py',
                       word_count_dir,
                       corpora_frequency_dir)

    # now, calculate tfidf scores
    tfidf_dir = 'tfidf'
    run_map_job('tf_idf_map.py {0}'.format(corpora_len),
                corpora_frequency_dir,
                tfidf_dir)

    # join on words for cosine similarity
    word_join_dir = 'joined_words'
    run_map_reduce_job('word_join_map.py',
                       'word_join_red.py',
                       tfidf_dir,
                       word_join_dir)

    # now, sum up the products to get the cosine similarities
    output_dir = "output"
    if len(sys.argv) > 2:
        output_dir = sys.argv[3]
    run_map_reduce_job('cos_sim_map.py',
                       'cos_sim_red.py',
                       word_join_dir,
                       output_dir)
