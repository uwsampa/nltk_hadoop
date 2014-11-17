#!/usr/bin/env python
from map_reduce_utils import clean_text

import sys

for line in sys.stdin:
    key, value = line.strip().split('\t')
    docname = key
    word, count = value.strip().split()
    # This is ... not very generic but not sure how to pass
    # variables from the script running all of the jobs to this one
    path = docname.split('file:', 1)[1]
    # also, would be better to not re-read file every time, but seems like
    # this is done in practice.

    try:
        contents = open(path, 'r').read()
        contents = clean_text(contents)
    except:
        with open('errors.txt', 'a+') as f:
            f.write(docname.split('file:', 1)[1])
    document_word_count = len(contents)
    print '%s %s\t%s %s' % (word, docname, count, document_word_count)
