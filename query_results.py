#!/usr/bin/env python

import argparse
import sqlite3
import collections
import map_reduce_utils as mru
from math import log
from create_db import *  # just for config/locations

def main():
    description = '''
                  given a sqlite3 database file produced using create_db.py,
                  either display the top k most similar documents in the corpus
                  or, if given a new file to analyze, calculate the k most
                  similar documents to it in the original corpus. This designed
                  to be run on smaller datasets just to gain insight/intuiton.
                  '''
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    top_help = "the number of similar documents to display"
    parser.add_argument('--top', '-t', help=top_help, type=int, default=5)

    file_help = '''find the k most similar documents to file instead of the
               entire corpus'''
    parser.add_argument('--file', '-f', dest='file', help=file_help)

    db_name_help = "the location of the sqlite database file to query"
    parser.add_argument('--database', '-d', dest='db_location',
                        help=db_name_help, default=DEFAULT_DB_LOCATION)

    args = parser.parse_args()
    with sqlite3.connect(args.db_location) as conn:
        db = conn.cursor()
        if args.file:
            compare_file(db, args.file, args.top)
        else:
            find_top_k(db, args.top)
        conn.commit()


def find_top_k(db, k):
    """
    prints to top k similar documents in the 'similarities' table
    of the database referenced by the sqlite3 cursor dbx
    """
    results = db.execute('''SELECT *
                            FROM similarities
                            ORDER BY similarity DESC
                            LIMIT ?;''', (k,))
    print '\n'.join(map(lambda x: '{} {} {:.10f}'.format(x[0], x[1], x[2]),
                        results.fetchall()))


def compare_file(db, filename, k):
    """
    finds the k most similar documents to filename in the database referenced
    by the sqlite3 cursor db and prints them along with the cosine similarity
    metric between filename and each of the k documents
    """
    with open(filename, 'r') as f:
        contents = f.read()
    contents = mru.clean_text(contents)
    counts = {}
    for word in contents:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    docs_containing = {w: num_docs_containing(db, w) for w in set(contents)}

    # we're going to use the number of documents in the original
    # corpus to calculate tfidf, not including the file we are now
    # analyzing, since the tfidf scores we have in the database were
    # calculated with this number
    corp_size = get_corpus_size(db)
    doc_size = len(contents)
    tfidfs = {word: tfidf(count, doc_size, corp_size, docs_containing[word])
              for word, count in counts.items()}

    # now, calculate the similarity metric with each document in the database
    similarities = {}
    documents = db.execute('SELECT DISTINCT document FROM tfidf;').fetchall()
    for doc in map(lambda x: x[0], documents):
        similarity = 0
        for word in set(contents):
            other_doc_tfidf = get_tfidf(db, doc, word)
            this_doc_tfidf = tfidfs[word]
            similarity += this_doc_tfidf * other_doc_tfidf
        similarities[doc] = similarity
    top_k = collections.Counter(similarities).most_common(k)
    print '\n'.join(map(lambda x: ':\t'.join([repr(i) for i in x]), top_k))


def tfidf(n, N, D, m):
    """
    given the document frequency n, the document length N, corpus size D
    and corpus frequency m, returns the tfidf score for this word and document
    """
    # Since we're using counts from the original corpus, it could be that
    # this word is unique to the document we are analyzing now, in which
    # case we need to do some (sort of) smoothing
    if m == 0:
        m = 1

    if (n == 0 or D == 0):
        return 0.0
    else:
        return (float(n) / float(N)) * log(float(D) / float(m), 10)


def get_tfidf(db, doc, word):
    """
    returns the tfidf score of word in the document named doc in the
    database referenced by the sqlite3 cursosr db
    """
    results = db.execute('''SELECT tfidf
                            FROM tfidf
                            WHERE word = ?
                            AND document = ?
                            LIMIT 1''', (word, doc)).fetchall()
    # if there are no results, then we simply return 0 so that nothing
    # is added to the similarity for this word
    if results == []:
        return 0.0
    else:
        return results[0][0]


def get_corpus_size(db):
    """
    returns the number of unique documents in the 'tfidf' table in
    the database referenced by the sqlite3 cursor db
    """
    results = db.execute('SELECT COUNT(distinct document) FROM tfidf;')
    return int(results.fetchall()[0][0])


def num_docs_containing(db, word):
    """
    returns the number of documents which contain word by querying
    the 'tfidf' table in the database referenced by the sqlite3
    cursor db
    """
    result = db.execute('''SELECT COUNT(distinct document)
                           FROM tfidf
                           WHERE document = ?;
                        ''', (word,))
    return int(result.fetchall()[0][0])


if __name__ == '__main__':
    main()
