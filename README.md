[![Build Status](https://travis-ci.org/uwsampa/nltk-hadoop.svg?branch=avro-refactor)](https://travis-ci.org/uwsampa/nltk-hadoop)
# Materialize an nltk corpus

- find a corpus at http://nltk.googlecode.com/svn/trunk/nltk_data/index.xml, e.g. `inaugural`

```sh
python materialize_nltk_corpus.py inaugral
```

# Set the appropriate environment variables
```sh
source ./hadoop-streaming-env.sh
```
or
```sh
export HADOOP_VERSION=  # the version of hadoop you are using, e.g. 2.5.1
export AVRO_VERSION=  # if you are using avro, the version, e.g. 1.7.7
export HADOOP_HOME=  # the location of your hadoop installation
export RELATIVE_PATH_JAR=  # location of hadoop streaming jar in HADOOP_HOME
export NLTK_HOME=  # the location of your corpus, mappers and reducers
export AVRO_JAR=  # if you are using avro, the jar location
```

you may also want to ensure that the mapper and reducer scripts are executable


# Run the MapReduce jobs to produce output
(note, this depends upon avro, nltk and scikit-learn)

```sh
./mapred_tfidf --input INPUT_DIR --output OUTPUT_DIR
```
* run with the `--help` flag to view all options
* run with `--force` to automatically overwrite intermediate directories

See the cosine similarities of all documents:
```sh
ls $OUTPUT_DIR/part-*
```

See the tfidf metrics for each document/word pair:
```sh
ls $tfidf/part-*
```

# Run the test suite

with `nose` installed,
```sh
nosetests
```

# Write a new map / reduce job and run it
Hadoop streaming accepts any command as a mapper or reducer, but to use the `map_reduce_utils` module, the basic pattern is as follows:

first, write a mapper like the abstract one below:

```python
#!/usr/bin/env python

import map_reduce_utils as mru
import sys


def mapper(input=sys.stdin, output=sys.stdout):
    for in_key, in_value in mru.json_loader(input):
        out_key = {}  # the key that is emitted by hadoop as json
        out_value = {}  # the value that is emitted by hadoop as json
        mru.mapper_emit(out_key, out_value, output)


if __name__ == '__main__':
   mapper()  # feel free to pass arguments here as well

```

then, write a reducer similar to:

```python
#!/usr/bin/env python

import map_reduce_utils as mru
import sys


def reducer(input=mru.reducer_stream(), output=sys.stdout):
    for in_key, key_stream in input:
        values = []  # will contain each value associated with in_key
        for in_value in key_stream:
            values.append(in_value)
        # now, values contains all of the values stored as Dicts, so we can
        # do our "reduction" with arbitrary python. note that you don't need to
        # store all of the in_values if, for example, we only need a running sum
        out_key = {}  # the key that is emitted by hadoop as json
        out_value = {}  # the value that is emitted by hadoop as json
        mru.reducer_emit(out_key, out_value, output)
        # you can also emit more than one key-value pairs here, for example
        # one for each key-value pair where key = in_key:
        for value in values:
            out_key = {} # the key that is emitted by hadoop as json
            out_value = {} # the value that is emitted by hadoop as json
            mru.reducer_emit(out_key, out_value, output)


if __name__ == '__main__':
   reducer()  # feel free to pass arguments here as well
```

now, in your main driver (let's call it `run_hadoop.py` for future reference),
invoke your mapper and reducer

```python
import  map_reduce_utils as mru

# input_dir contains the lines piped into the reducer, output_dir is where the
# results will be placed.
mru.run_map_reduce_job('mapper.py', 'reducer.py', input_dir, output_dir)

# note that we can pass arguments or arbitrary commands as mappers and reducers
# and use the output of one job as the input of the next job to chain MR jobs

mru.run_map_reduce_job('second_mapper.py --arg 1', 'wc -l',
                        output_dir, second_MR_job_output_dir)

```

Before running the previous code, however, remember to define the
appropriate environment variables. For example, in a shell, run:
```sh
source hadoop-streaming-env.sh
python run_hadoop.py
```

Note that
* You don't need to use avro and json. If you want, you can specify the input and output format when invoking `map_reduce_utils.run_map_reduce_job`, as well as the tokenizers for the generators in both the mapper and reducer.
* You can run just a map job (i.e. no reducer) with `map_reduce_utils.run_map_job`
* To see a concrete example of a mapper and reduer, look at `word_join_map.py` and `word_join_red.py`.
* To see a concrete example of invoking a hadoop job, look at `mapred_tfidf.py`

# The TFIDF Metric
After cleaning and stemming a document, we obtain a list of words, `d`, for that document. The tfidf score of a word `w` in `d` is defined as follows:
* let `n` be the number of times `w` appears in `d`
* let `N` be the length of `d`
* let `D` be the number of documents in the corpus
* let `m` be the number of documents in which the word `d` appears at least once
* `tf = n / N` (tf is the 'term frequency' of the word)
* `idf = log(D / m)` (idf is the 'inverse document frequency' of the word)
* `tfidf = tf*idf`

These naming conventions are used in certain places in the codebase, for example in the docstrings for many mapper and reducer functions.
