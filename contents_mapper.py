#!/usr/bin/env python
import os
import sys
from map_reduce_utils import clean_text

for line in sys.stdin:
    docname = os.environ['mapreduce_map_input_file']
    contents = clean_text(line)
    print docname, '\t', ' '.join(map(str, contents))
