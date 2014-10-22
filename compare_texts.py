import os
import nltk
import string
import sys
import pylab
import numpy as np
import networkx as nx
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem.porter import PorterStemmer

def get_texts(dir):
    texts = {}

    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            path = subdir + os.path.sep + file
            fd = open(path, 'r')
            content = fd.read().lower().translate(None, string.punctuation)
            texts[file] = content
    return texts

def stem_tokens(src, stemmer):
    stemmed = []
    for item in src:
        stemmed.append(stemmer.stem(item))
    return stemmed

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = stem_tokens(tokens, PorterStemmer())
    return stems

def get_similarities(texts):
    tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english')
    tfs = tfidf.fit_transform(texts.values())
    similarities = [cosine_similarity(tfs[i:i+1], tfs) for i in range(tfs.shape[0])]
    return np.asmatrix(np.asarray(similarities))

def get_similarity_graph(similarities):
    # first, remove unneeded edges
    upper_similarities = np.triu(similarities)
    np.fill_diagonal(upper_similarities, 0)
    return nx.from_numpy_matrix(upper_similarities)

def plot_graph(g):
    pos = nx.spring_layout(g)
    pylab.figure(1)
    nx.draw(g, pos)
    edge_labels = dict([((u, v), d['weight']) for u, v, d in g.edges(data=True)])
    nx.draw_networkx_edge_labels(g, pos, edge_labels=edge_labels)
    pylab.show()


if __name__ == '__main__':

    input_dir = './corpus'
    if len(sys.argv) >= 2:
        input_dir = sys.argv[1]
    texts = get_texts(input_dir)
    similarities = get_similarities(texts)
    sim_graph = get_similarity_graph(similarities)
    plot_graph(sim_graph)
