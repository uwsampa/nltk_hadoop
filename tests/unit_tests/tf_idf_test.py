#!/usr/bin/env python

from math import log
import tests.test_utils as tu
import map_reduce_utils as mru
import tf_idf_map as map


class TestTfIdfMapper(tu.MapTestCase):
    """ tests the tf-idf mapper """

    def __init__(self, *args, **kwargs):
        super(TestTfIdfMapper, self).__init__(
            map.map_tf_idf,
            'tests/fixtures/tf_idf_mapper.txt',
            *args, **kwargs
        )
        self.default_args = {'corpus_size': 3, 'precision': 5}

    def test_has_correct_number_of_keys_and_values(self):
        """
        ensures that the correct number of keys and values are emitted
        """
        self.has_correct_number_of_keys_and_values(2, 1, args=self.default_args)

    def test_computes_correct_tfidf_score(self):
        """
        tests that the correct tfidf value is emitted
        """
        results = self.run_mapper(args=self.default_args)
        results = mru.tokenize_key_value_pair(results[0])
        computed_tfidf = float(results[1][0])
        expected_tf = (7.0 / 12.0)
        expected_idf = log((float(self.default_args['corpus_size']) / 2.0), 10)
        expected_tfidf = expected_tf * expected_idf
        expected_tfidf = round(expected_tfidf, self.default_args['precision'])
        self.assertEqual(expected_tfidf, computed_tfidf)

    def test_has_correcct_precision(self):
        """
        tests to ensure that the precision argument is ensured.
        """
        # we split on '.' and then check the length of the string after
        # the period
        precision_to_test = 8
        results = self.run_mapper(args={'precision': precision_to_test,
                                        'corpus_size': 9})
        result = mru.tokenize_key_value_pair(results[0])
        computed_tfidf = result[1][0]
        computed_precision = len(computed_tfidf.strip().split('.')[1])
        self.assertEqual(precision_to_test, computed_precision)
