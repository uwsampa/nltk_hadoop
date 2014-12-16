#!/usr/bin/env python

import os
import sys
from map_reduce_utils import clean_text


def map_contents(input=sys.stdin):
    """
    (file_contents) --> (file_name) (file_contents)

    for each line from stdin consisting of a document in the corpus, emits
    a key-value pair to stdout with a key of the corresponding filename
    and a value of the file contents cleaned with
    map_reduce_utils.clean_text
    """
    for line in input:
        docname = os.environ['mapreduce_map_input_file']
        contents = clean_text(line)
        print docname, '\t', ' '.join(map(str, contents))


if __name__ == '__main__':
    map_contents()
