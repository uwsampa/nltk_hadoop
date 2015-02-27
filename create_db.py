#!/usr/bin/env python

from __future__ import print_function
import argparse
import sqlite3
import os
import re

IS_MAP_REDUCE_OUTPUT = re.compile('^part-.*$')

DEFAULT_DB_LOCATION = 'tfidf.db'


def main(database_name, tfidf_location, similarities_location):
    """
    populates tables in a sqlite3 database named database_name containing
    the similarities and tfidf scores produced by mapred_tfidf.py. note that
    this is designed for smaller datasets, not the entire patent dataset.
    """
    with sqlite3.connect(database_name) as conn:
        db = conn.cursor()
        # yay denormalization
        populate_tfidf(db, tfidf_location)
        populate_simimlarities(db, similarities_location)
        conn.commit()


def populate_tfidf(db, input_dir):
    """
    creates and populates a table in the database referenced by the
    cursor db named tfidf using the content of input_dir,
    which should contain the output from the tfidf mapper
    from mapred_tfidf.py
    """
    db.execute('CREATE TABLE tfidf (word text, document text, tfidf real);')
    for subdir, dirs, files in os.walk(input_dir):
        for filename in files:
            # ignore "Success" and CRC files from mapred
            if IS_MAP_REDUCE_OUTPUT.match(filename):
                for line in open(input_dir + '/' + filename, 'r'):
                    vals = line.strip().split()
                    sql = 'INSERT INTO tfidf VALUES (?, ?, ?);'
                    db.execute(sql, [vals[0], vals[1], vals[2]])


def populate_simimlarities(db, input_dir):
    """
    creates and populates a table in the database referenced by the
    cursor db named similarities using the content of input_dir,
    which should contain the output from the similarity reducer
    from mapred_tfidf.py
    """
    # Since as of right now, we're only going to use this to query
    # top k similar documents, it doesn't matter which order
    # the documents are in.
    db.execute('''CREATE TABLE similarities
                  (doc1 text, doc2 text, similarity real);''')
    for subdir, dirs, files in os.walk(input_dir):
        for filename in files:
            if IS_MAP_REDUCE_OUTPUT.match(filename):
                for line in open(input_dir + '/' + filename, 'r'):
                    vals = line.strip().split()
                    sql = 'INSERT INTO similarities VALUES (?, ?, ?);'
                    db.execute(sql, (vals[0], vals[1], vals[2]))


if __name__ == '__main__':
    description = ''' uses the output of mapred_tfidf to produce a sqlite3
                      database which can be used to query the  most similar
                      documents in a corpus and compare a new text to the
                      documnets already processed with mapred_tfidf.
                  '''

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    db_name_help = 'the name of the sqlite3 db file to create'
    parser.add_argument('--output', '-o', default=DEFAULT_DB_LOCATION,
                        dest='db_name', help=db_name_help)

    tfidf_location_help = 'the location of the tf-idf output from mapred_tfidf'
    parser.add_argument('--tfidf', '-t', dest='tfidf_loc',
                        default='tfidf', help=tfidf_location_help)

    sim_loc_help = 'the location of the similarities output from mapred_tfidf'
    parser.add_argument('--similarities', '-s', dest='similarities_location',
                        default='similarities', help=sim_loc_help)

    force_help = 'if provided, automatically overwrite db_name if it exists'
    parser.add_argument('--force', '-f', dest='force', default=False,
                        help=force_help, action='store_true')

    args = parser.parse_args()
    db_name = args.db_name
    tfidf_location = args.tfidf_loc
    similarities_location = args.similarities_location
    force = args.force

    # don't clobber the db location unless user asks to
    if os.path.exists(db_name):
        if not force:
            prompt = 'Overwrite db file {} [y/n]?'.format(db_name)
            response = raw_input(prompt)
            if response not in ['y', 'yes', 'Y', 'Yes']:
                exit()

        try:
            os.remove(db_name)
        except OSError as ose:
            err_msg = 'unable to rm db file. is it a dir? chmod?'
            print(err_msg, file=os.stderr)

    main(db_name, tfidf_location, similarities_location)
