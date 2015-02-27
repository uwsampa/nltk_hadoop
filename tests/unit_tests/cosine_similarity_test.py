#!/usr/bin/env python

import tests.test_utils as tu
import cos_sim_map as map
import cos_sim_red as red


class TestCosineSimilarityMapper(tu.MapTestCase):
    """ tests the cosine similarity mapper """

    def __init__(self, *args, **kwargs):
        super(TestCosineSimilarityMapper, self).__init__(
            map.map_cosine_similarity,
            'tests/fixtures/cosine_similarity_mapper.txt',
            *args, **kwargs
        )

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the same number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 1)

    def test_emits_line_for_each_word_in_each_file(self):
        """
        ensures that the same number of lines of input and output are emitted
        """
        self.lines_out_equals_lines_in()


class TestCosineSimilarityReducer(tu.ReduceTestCase):
    """ tests the cosine simiarity reducer """

    def __init__(self, *args, **kwargs):
        super(TestCosineSimilarityReducer, self).__init__(
            red.KEYS, red.VALUES,
            red.reduce_cosine_similarity,
            'tests/fixtures/cosine_similarity_reducer.txt',
            *args, **kwargs
        )
        self.default_args = {'precision': 6}

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the same number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 1, args=self.default_args)

    def test_sums_up_correct_values(self):
        """
        ensures that the correct sum is computed
        """
        for key, value in self.run_reducer_tokenize(args=self.default_args):
            if key[0] == 'file1' and key[1] == 'file2':
                self.assertEqual(value[0], '1.000000')
            else:
                self.assertEqual(value[0], '1.500000')
