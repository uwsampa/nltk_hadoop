#!/usr/bin/env python
import sys

"""
(word) (file_name tfidf) --> (word) (file1 file2 tfidf1*tfidf2)

for each word, if two distinct documents both contain that word,
a line is emitted containing the product of the tfidf scores of that
word in both documents.

This is the first step in computing the pairwise dot product of the tf-idf
vectors between all documents, where the corresponding elements for every
pair of documents are multiplied together.
"""

cur_word = None
word = None
matching_docs = []


def print_results(docs, word):
    for doc1 in docs:
        for doc2 in docs:
            if doc1 != doc2:
                print '%s\t%s %s %.16f' % (word, doc1[0], doc2[0],
                                           doc1[1]*doc2[1])


for line in sys.stdin:
    key, value = line.strip().split('\t')
    word = key.strip()
    filename, tfidf = value.strip().split()
    tfidf = float(tfidf)
    if word == cur_word:
        matching_docs.append((filename, tfidf))
    else:
        if cur_word is not None:
            print_results(matching_docs, cur_word)
        cur_word = word
        matching_docs = []
        matching_docs.append((filename, tfidf))

if cur_word is not None:
    print_results(matching_docs, cur_word)
