#!/usr/bin/env python

import tests.test_utils as tu
import word_join_map as map
import word_join_red as red


class TestWordJoinMapper(tu.MapTestCase):
    """ tests the word join mapper """

    def __init__(self, *args, **kwargs):
        super(TestWordJoinMapper, self).__init__(
            map.map_word_join,
            'tests/fixtures/word_join_mapper.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(1, 2)

    def test_lines_out_equals_lines_in(self):
        """
        ensures that the number of lines of input and output are equal
        """
        self.lines_out_equals_lines_in()


class TestWordJoinReducer(tu.ReduceTestCase):
    """ tests the word join reducer """

    def __init__(self, *args, **kwargs):
        super(TestWordJoinReducer, self).__init__(
            red.KEYS, red.VALUES,
            red.reduce_word_join,
            'tests/fixtures/word_join_reducer.txt',
            *args, **kwargs
        )
        self.default_args = {'precision': 6}

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(1, 3, args=self.default_args)

    def test_emits_correct_number_of_lines(self):
        """
        ensures that the correct number of lines are emitted
        """
        output_size = len(self.run_reducer(args=self.default_args))
        self.assertEqual(output_size, 8)

    def test_correct_values_are_computed(self):
        """
        ensures that the correct values are calculated for a small
        sample data set.
        """
        for result in self.run_reducer_tokenize(args=self.default_args):
            if result[0][0] == 'word1':
                if result[1][0] == 'file1':
                    if result[1][1] == 'file2':
                        self.assertEqual(float(result[1][2]), .5)
                    else:
                        self.fail('wrong filename')
                elif result[1][0] == 'file2':
                    if result[1][1] == 'file1':
                        self.assertEqual(float(result[1][2]), .5)
                    else:
                        self.fail('wrong filename')
                else:
                    self.fail('wrong filename')
            elif result[0][0] == 'word2':
                if result[1][0] == 'file1':
                    if result[1][1] == 'file2':
                        self.assertEqual(float(result[1][2]), .25)
                    elif result[1][1] == 'file3':
                        self.assertEqual(float(result[1][2]), .05)
                    else:
                        self.fail('wrong filename')
                elif result[1][0] == 'file2':
                    if result[1][1] == 'file1':
                        self.assertEqual(float(result[1][2]), .25)
                    elif result[1][1] == 'file3':
                        self.assertEqual(float(result[1][2]), .05)
                    else:
                        self.fail('wrong filename')
                elif result[1][0] == 'file3':
                    if result[1][1] == 'file1':
                        self.assertEqual(float(result[1][2]), .05)
                    elif result[1][1] == 'file2':
                        self.assertEqual(float(result[1][2]), .05)
                    else:
                        self.fail('wrong filename')
                else:
                    self.fail('wrong filename')
            else:
                self.fail('wrong word in output')
