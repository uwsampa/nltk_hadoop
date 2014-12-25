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
export HADOOP_HOME=  # the location of your hadoop installation
export RELATIVE_PATH_JAR=  # location of hadoop streaming jar in HADOOP_HOME
export NLTK_HOME=  # the location of your corpus, mappers and reducers
```

you may also want to ensure that the mapper and reducer scripts are executable


# Run the MapReduce jobs to produce output

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


# The TFIDF Metric
After cleaning and stemming a document, we obtain a list of words, `d`, for that document. The tfidf score of a word `w` in `d` is defined as follows:
* let `n` be the number of times `w` appears in `d`
* let `N` be the length of `d`
* let `D` be the number of documents in the corpus
* let `m` be the number of documents in which the word `d` appears at least once
* `tf = n / N` (tf is the 'term frequency' of the word)
* `idf = D / m` (idf is the 'inverse document frequency' of the word)
* `tfidf = tf*idf`

These naming conventions are used in certain places in the codebase, for example in the docstrings for many mapper and reducer functions.
