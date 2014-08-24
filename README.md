# Materialize an nltk corpus

- find a corpus at http://nltk.googlecode.com/svn/trunk/nltk_data/index.xml, e.g. `inaugural`

```sh
python materialize_nltk_corpus.py inaugral
```

# Run tagger on hadoop streaming

```sh
./hadoop-tag.sh inaugural
```

See the output
```sh
ls out/part-*
```
