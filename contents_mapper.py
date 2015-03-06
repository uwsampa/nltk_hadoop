#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import map_reduce_utils as mru

"""
(file_contents) --> (file_name) (file_contents)

for each line from stdin consisting of a document in the corpus, emits
a key-value pair to stdout with a key of the corresponding filename
and a value of the file contents cleaned with
map_reduce_utils.clean_text
"""


def map_contents(input=sys.stdin, output=sys.stdout):
    for line in input:
        docname = os.environ['mapreduce_map_input_file']
        contents = mru.clean_text(line)
        key = {'filename': docname}
        value = {'words': [word for word in contents]}
        # we emit as if we were a reducer since the contents don't get put
        # through a reducer
        mru.reducer_emit(key, value, output)


if __name__ == '__main__':
    map_contents()
