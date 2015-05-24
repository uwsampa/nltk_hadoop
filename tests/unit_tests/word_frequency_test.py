#!/usr/bin/env python

import re
import tests.test_utils as tu
import word_freq_map as map
import word_freq_red as red


class TestWordFrequencyMapper(tu.MapTestCase):
    """
    tests basic functionality of the word frequency mapper
    """

    def __init__(self, *args, **kwargs):
        super(TestWordFrequencyMapper, self).__init__(
            map.map_word_frequency,
            'tests/fixtures/word_frequency_mapper.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 1)

    def test_emits_line_for_each_word_in_each_file(self):
        """
        ensures that a line is emitted for each word in each file
        """
        with open(self.default_fixture) as f:
            output = self.run_mapper()
            input = f.readlines()
            num_files = len(input)
            total_words = 0
            for line in input:
                total_words += len(line.strip().split())
            self.assertEqual(len(output), total_words - num_files)

    def test_appends_one(self):
        """
        tests that 1 is appended to each line of output
        """
        ends_with_one = '.*1$'
        self.are_all_matches(re.compile(ends_with_one))


class TestWordFrequencyReducer(tu.ReduceTestCase):
    """ tests the word frequency reducer """

    def __init__(self, *args, **kwargs):
        super(TestWordFrequencyReducer, self).__init__(
            red.KEYS, red.VALUES,
            red.reduce_word_frequency,
            'tests/fixtures/word_frequency_reducer.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 1)

    def test_sum_of_output_equals_length_of_input(self):
        """
        tests that each line is accounted for in the sum produced as output
        """
        output_total_sum = 0
        for line in self.run_reducer_tokenize():
            output_total_sum += int(line[1][0])
        with open(self.default_fixture) as f:
            input_len = len(f.readlines())
            self.assertEqual(output_total_sum, input_len)
