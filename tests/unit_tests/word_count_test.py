#!/usr/bin/env python

import tests.test_utils as tu
import word_count_map as map
import word_count_red as red


class TestWordCountMapper(tu.MapTestCase):
    """ tests the word count mapper """

    def __init__(self, *args, **kwargs):
        super(TestWordCountMapper, self).__init__(
            map.map_word_count,
            'tests/fixtures/word_count_mapper.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are returned
        """
        self.has_correct_number_of_keys_and_values(1, 2)

    def test_lines_out_equals_lines_in(self):
        """
        ensures that a line is emmited for each line of input
        """
        self.lines_out_equals_lines_in()


class TestWordCountReducer(tu.ReduceTestCase):
    """ tests the word count reducer """

    def __init__(self, *args, **kwargs):
        super(TestWordCountReducer, self).__init__(
            red.KEYS, red.VALUES,
            red.reduce_word_count,
            'tests/fixtures/word_count_reducer.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 2)

    def test_sums_up_correct_values(self):
        """
        ensures that the correct sum is calculated
        """
        for result in self.run_reducer_tokenize():
            if result[0][1] == 'file1':
                self.assertEqual(int(result[1][1]), 11)
            elif result[0][1] == 'file2':
                self.assertEqual(int(result[1][1]), 16)
            elif result[0][1] == 'file3':
                self.assertEqual(int(result[1][1]), 26)
            else:
                self.fail('unknown filename')

    def test_lines_out_equals_lines_in(self):
        """
        test to ensure that the number of lines of input is equal to
        the number of lines of output.
        """
        self.lines_out_equals_lines_in()
