#!/usr/bin/env python

import hdfs.ext.avro
import hdfs


"""
note that since these helper functions use http, they are designed for simple
(slow) use cases like pulling results of MR jobs on the cluster to another
location, so don't expect to get hdfs/Infiniband performance.

see http://hdfscli.readthedocs.org/en/latest/api.html#module-hdfs.client
"""


HDFS_DEFAULT_PORT = 50070
SAMPA_HDFS_URL = 'http://hadoop.sampa:{}'.format(HDFS_DEFAULT_PORT)

LOCALHOST_HDFS_URL = 'http://localhost:{}'.format(HDFS_DEFAULT_PORT)


def hdfs_client_connection(url=SAMPA_HDFS_URL, root='/'):
    client = hdfs.client.InsecureClient(url, root='/')
    return client


def hdfs_file_contents(filepath, url=SAMPA_HDFS_URL, buffer_char='\n'):
    """
    generates each part of each file in filepath, yielding whenever it sees
    a <buffer_char>
    """
    client = hdfs_client_connection(url=url)
    for line in client.read(filepath, buffer_char=buffer_char):
        yield line
    raise StopIteration()


def hdfs_dir_contents(directory, url=SAMPA_HDFS_URL, buffer_char='\n'):
    """
    performs hdfs_file_contents for each part-XYZ file in directory
    """
    client = hdfs_client_connection(url=url)
    part_files = client.parts(directory)
    for part_file in part_files:
        for line in hdfs_file_contents(part_file):
            yield line
    raise StopIteration()


def hdfs_append_to_file(filepath, text_to_append, url=SAMPA_HDFS_URL):
    """
    appends text_to_append to filepath at url
    """
    hdfs_touch_file(filepath, url=url)
    client = hdfs_client_connection(url=url)
    response = client.append(filepath, text_to_append)
    if not response.ok:
        raise Exception("failed to append:" + response.reason)


def hdfs_write_to_file(filepath, content, url=SAMPA_HDFS_URL):
    """
    this overwrites the contents currently in filepath with content
    """
    # may need to set overwrite option here
    client = hdfs_client_connection(url=url)
    response = client.write(filepath, content)
    if not response.ok:
        raise Exception("failed to write to file:" + response.reason)


def hdfs_touch_file(filepath, url=SAMPA_HDFS_URL):
    """
    ensures that filepath exists, creates it if neccessary
    """
    try:
        hdfs_append_to_file(filepath, '', url)
    except hdfs.util.HdfsError as e:
        if str(e) == 'File /{} not found.'.format(filepath):
            # write creates neccessary dirs / files
            hdfs_write_to_file(filepath, '', url)
        else:
            raise e


def hdfs_avro_records(filepath, url=SAMPA_HDFS_URL):
    client = hdfs_client_connection(url=url)
    avro_reader = hdfs.ext.avro.AvroReader(client, filepath)
    for record in avro_reader.records:
        yield record
    raise StopIteration()


def hdfs_make_directory(filepath, url=SAMPA_HDFS_URL):
    client = hdfs_client_connection(url=url)
    response = client._create(filepath)
    if not response.ok:
        raise Exception("failed to mkdir:" + response.reason)


def hdfs_remove_directory(filepath, url=SAMPA_HDFS_URL):
    client = hdfs_client_connection(url=url)
    response = client._delete(filepath)
    if not response.ok:
        raise Exception("failed to rmdir:" + response.reason)


def main():
    # example usage:

    # first, we'll read the contents of a file
    # hdfs_filepath = 'patents/claims_splits/claims.tsv.aa'
    # lines = hdfs_file_contents(hdfs_filepath)
    # first_line = lines.next()
    # print first_line

    # now let's try to write
    # hdfs_append_to_file('user/zbsimon/temp.txt', 'can I append here?')

    # now, try to create new file
    # hdfs_touch_file('user/zbsimon/newfile.txt')

    # now, read content of entire directory
    # hdfs_filepath = 'patents/output/json/ngrams'
    # lines = hdfs_dir_contents(hdfs_filepath)
    # first_line = lines.next()
    # print first_line
    # now, do an avro file
    # a = hdfs_avro_records_file_content('patents/output/tfidf')
    # print a.next()
    pass


if __name__ == '__main__':
    main()
