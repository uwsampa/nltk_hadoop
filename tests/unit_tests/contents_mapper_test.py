#!/usr/bin/env python

import tests.test_utils as tu
import contents_mapper
import os
import re


class TestContentsMapper(tu.MapTestCase):
    """ tests contents mapper """

    def __init__(self, *args, **kwargs):
        super(TestContentsMapper, self).__init__(
            contents_mapper.map_contents,
            'tests/fixtures/contents_mapper.txt',
            *args, **kwargs
        )
        os.environ['mapreduce_map_input_file'] = 'filename'

    def test_has_correct_number_of_keys_and_values(self):
        "ensures that the correct number of keys and values are emitted"
        self.has_single_delimiter()
        self.has_n_keys(1)

    def test_only_lowercase_alphabetic(self):
        "ensures all content is lowercase alphabetic charachters"
        numerals = re.compile('^[a-z, ,\t]+$')
        self.are_all_matches(numerals)

    def test_prepends_filename(self):
        "ensures that the filename is prepended to each line emitted"
        contains_filename = re.compile('^filename')
        self.are_all_matches(contains_filename)
