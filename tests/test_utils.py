#!/usr/bin/env python

import unittest
import re
import map_reduce_utils as mru

"""
utility functions/classes to help in testing
"""

# templates for regexes
single_delimiter = '^[^{0}]*{0}[^{0}]*$'
n_plus_one_values = '.*\t([^ ]+ ){{{}}}[^ ]+$'
n_plus_one_keys = '^([^ ]+ ){{{}}}[^ ]+\t.*'


class OutputBuffer(object):
    """
    a simple wrapper for a list that can be passed as the file parameter
    to print() so we can use it as the output destination for mappers
    and reducers while testing
    """

    def __init__(self):
        """
        constructs a new empty output buffer
        """
        self.buf = []

    def write(self, str):
        """
        adds str to this output buffer. this method doesn't append
        newlines to the buffer since print() results in another implicit
        call to print with only a newline. rather than passing end="" to
        newline, we just drop them here
        """
        if str != '\n':
            self.buf.append(str)

    def contents(self):
        """
        returns the contents of this output buffer
        """
        return self.buf


class MapTestCase(unittest.TestCase):
    """
    base class with common functionality for tests which test a map function.

    note that method names purpusefully do not match nosetest's "test" regex,
    so MapTestCase will not be instantiated it unless you subclass it. futher,
    none of its methods will actually be run by nose, so you need to explicitly
    invoke them in your subclass.
    """

    def __init__(self, function_to_test, default_fixture, *args, **kwargs):
        """
        constructs a new MapTestCase that tests function_to_test and will
        default to using default_fixture in its tests.
        """
        self.to_test = function_to_test
        self.default_fixture = default_fixture
        super(MapTestCase, self).__init__(*args, **kwargs)

    def run_mapper(self, fixture=None, args={}):
        """
        runs the function to test provided in the constructor on fixture
        and returns the resulting output as a list. self.default_fixture
        is used if no fixture is provided.
        """
        if fixture is None:
            fixture = self.default_fixture
        with open(fixture, 'r') as f:
            args['input'] = f
            return pipe_through(self.to_test, args)

    def are_no_matches(self, regex, fixture=None, args={}):
        """
        ensures that no line of output from run_mapper on fixture match regex
        """
        results = self.run_mapper(fixture, args)
        for line in results:
            self.assertFalse(regex.match(line))

    def are_all_matches(self, regex, fixture=None, args={}):
        """
        ensures that all lines of output from run_mapper on fixture match regex
        """
        results = self.run_mapper(fixture, args)
        for line in results:
            self.assertTrue(regex.match(line))

    def has_n_keys(self, n, fixture=None, args={}):
        """
        ensures all lines of output from run_mapper on fixture have n keys
        """
        template = n_plus_one_keys.format(n - 1)
        self.are_all_matches(re.compile(template), fixture, args)

    def has_n_values(self, n, fixture=None, args={}):
        """
        ensures all lines of output from run_mapper on fixture have n values
        """
        template = n_plus_one_values.format(n - 1)
        self.are_all_matches(re.compile(template), fixture, args)

    def has_single_delimiter(self, fixture=None, delimiter='\t', args={}):
        """
        ensures all lines of output from run_mapper on fixture have 1 delimiter
        """
        regex = re.compile(single_delimiter.format(delimiter))
        self.are_all_matches(regex, fixture, args)

    def has_correct_number_of_keys_and_values(self, num_keys, num_values,
                                              fixture=None, args={}):
        """
        ensures that all lines of output from run_mapper on fixture have
        num_keys keys, num_values values and a single tab
        """
        self.has_n_keys(num_keys, fixture, args)
        self.has_n_values(num_values, fixture, args)
        self.has_single_delimiter(fixture, args=args)

    def lines_out_equals_lines_in(self, fixture=None, args={}):
        """
        tests that the number of lines of input and output are equal
        when the reducer is run using fixture and args
        """
        num_output = len(self.run_mapper(fixture, args))
        with open(self.default_fixture) as f:
            num_input = len(f.readlines())
        self.assertEqual(num_input, num_output)


class ReduceTestCase(unittest.TestCase):
    """
    base class with common functionality for tests which test reducer functions

    note that method names purpusefully do not match nosetest's "test" regex,
    so ReduceTestCase will not be instantiated it unless you subclass it.
    futher, none of its methods will actually be run by nose, so you need to
    explicitly invoke them in your subclass.
    """

    def __init__(self, keys, values, function_to_test, fixture, *args, **kwargs):
        """
        constructs a new ReduceTestCase that tests function_to_test, which
        expects input to have keys corresponding to keys and values
        corresponding to values, and uses fixture as the default fixture for
        input if no input file is specified explicitly
        """
        self.keys = keys
        self.values = values
        self.to_test = function_to_test
        self.default_fixture = fixture
        self.kv_to_dict = mru.KeyValueToDict(self.keys, self.values).to_dict
        super(ReduceTestCase, self).__init__(*args, **kwargs)

    def run_reducer(self, fixture=None, args={}):
        """
        runs the reducer that is being tested on fixture with arguments args
        and returns the resulting output strings in a list.
        """
        if fixture is None:
            fixture = self.default_fixture
        with open(fixture, 'r') as f:
            args['input'] = mru.reducer_stream(self.keys, self.values, f.readline)
            return pipe_through(self.to_test, args)

    def run_reducer_tokenize(self, fixture=None, args={},
                             tokenizer=mru.tokenize_key_value_pair):
        """
        the same as run_reducer, but output is tokenized with tokenizer
        before being returned.
        """
        return [tokenizer(i) for i in self.run_reducer(fixture, args)]

    def are_no_matches(self, regex, fixture=None, args={}):
        """
        ensures no lines of output from runnning the reducer match regex
        when run using fixture and args
        """
        results = self.run_reducer(fixture, args)
        for line in results:
            self.assertFalse(regex.match(line))

    def are_all_matches(self, regex, fixture=None, args={}):
        """
        ensures all lines of output from runnning the reducer match regex
        when run using fixture and args
        """
        results = self.run_reducer(fixture, args)
        for line in results:
            self.assertTrue(regex.match(line))

    def has_n_keys(self, n, fixture=None, args={}):
        """
        ensures that each line of output from running reducer has n keys
        when run using fixture and args
        """
        template = n_plus_one_keys.format(n - 1)
        self.are_all_matches(re.compile(template), fixture, args)

    def has_n_values(self, n, fixture=None, args={}):
        """
        ensures that each line of output from running reducer has n values
        when run using fixture and args
        """
        template = n_plus_one_values.format(n - 1)
        self.are_all_matches(re.compile(template), fixture, args)

    def has_single_delimiter(self, fixture=None, delimiter='\t', args={}):
        """
        ensures that each line of output from running reducer has 1 delimiter
        when run using fixture and args
        """
        regex = re.compile(single_delimiter.format(delimiter))
        self.are_all_matches(regex, fixture, args)

    def has_correct_number_of_keys_and_values(self, num_keys, num_values,
                                              fixture=None, args={}):
        """
        ensures each line of output has num_key keys and num_values values
        and a single tab.
        """
        self.has_n_keys(num_keys, fixture, args)
        self.has_n_values(num_values, fixture, args)
        self.has_single_delimiter(fixture, args=args)

    def lines_out_equals_lines_in(self, fixture=None, args={}):
        """
        tests that the number of lines of input and output are equal
        when run using fixture and args
        """
        num_output = len(self.run_reducer(fixture, args))
        with open(self.default_fixture) as f:
            num_input = len(f.readlines())
        self.assertEqual(num_input, num_output)


def pipe_through(function, args={}):
    """
    returns the output resulting from invoking function with args
    where function is either a mapper or reducer that expects
    a file-like object to print output to
    """
    result = OutputBuffer()
    args['output'] = result
    function(**args)
    return result.contents()
