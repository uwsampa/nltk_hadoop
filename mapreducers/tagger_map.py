#!/sampa/home/bdmyers/escience/python/install/bin/python
import sys
import os
import nltk
import re

# get document name
docname = os.environ["mapreduce_map_input_file"]

sent_delims = re.compile('|'.join(['\.', ';']))

sentences = re.split(sent_delims, sys.stdin.read().replace('\n', ' '))[:-1]

for s in sentences:
  tokens = nltk.word_tokenize(s)
  tagged = nltk.pos_tag(tokens)
  print "%s\t%s" % (docname, tagged)
