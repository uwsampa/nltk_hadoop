#!/usr/bin/env python

import tests.test_utils as tu
import corp_freq_map as map
import corp_freq_red as red
import re


class TestCorpusFrequencyMapper(tu.MapTestCase):
    """
    tests corpus frequency mapper
    """

    def __init__(self, *args, **kwargs):
        super(TestCorpusFrequencyMapper, self).__init__(
            map.map_corpus_frequency,
            'tests/fixtures/corpus_frequency_mapper.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        tests that values emitted have correct number of keys and values
        """
        self.has_correct_number_of_keys_and_values(1, 4)

    def test_appends_one(self):
        """
        tests that 1 is appended to each line of input
        """
        ends_with_one = '.*1$'
        self.are_all_matches(re.compile(ends_with_one))

    def test_lines_out_equals_lines_in(self):
        """
        tests that the number of lines of input and output are equal
        """
        self.lines_out_equals_lines_in()


class TestCorpusFrequencyReducer(tu.ReduceTestCase):
    """
    tests basic functionality of the corpus frequency reducer
    """

    def __init__(self, *args, **kwargs):
        super(TestCorpusFrequencyReducer, self).__init__(
            red.KEYS, red.VALUES,
            red.reduce_corpus_frequency,
            'tests/fixtures/corpus_frequency_reducer.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 3)

    def test_sums_correct_values(self):
        """
        ensure that the sum is the number of document a word occurs in,
        not the number of occurences of the word.
        """
        for line in self.run_reducer_tokenize():
            if line[0][0] == 'word1':
                self.assertEqual(int(line[1][2]), 2)
            elif line[0][0] == 'word2':
                self.assertEqual(int(line[1][2]), 2)
            elif line[0][0] == 'word3':
                self.assertEqual(int(line[1][2]), 1)
            else:
                self.fail('unknown word in output')

    def test_lines_out_equals_lines_in(self):
        """
        tests that the number of lines of input and output are equal
        """
        self.lines_out_equals_lines_in()
