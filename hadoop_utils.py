#!/usr/bin/env python

import avro.io
import hdfs
import json


"""
I'm putting stuff in here rather than map_reduce_utils.py for now because
1) We have utils that are for hadoop generally, so better name
2) map_reduce_utils gets used a lot, so dont' want to mess w/ it

"""

SAMPA_HDFS_URL = 'hdfs://hadoop.sampa'


def hdfs_file_contents(filepath, url=SAMPA_HDFS_URL):
    """
    generates each part of each file in filepath
    """
    client = hdfs.client.InsecureClient(url)
    for line in client.read(filepath, buffer_char='\n'):
        yield line
    raise StopIteration()


def hdfs_append_to_file(filepath, text_to_append, url=SAMPA_HDFS_URL):
    client = hdfs.client.InsecureClient(url)
    client.append(filepath, text_to_append)
