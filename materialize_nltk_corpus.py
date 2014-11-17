import nltk
import os
import errno
import sys
import nltk.corpus

corpusname = "inaugural"
if len(sys.argv) >= 2:
    corpusname = sys.argv[1]

filelim = 4
if len(sys.argv) >= 3:
    filelim = int(sys.argv[2])

corpus = getattr(nltk.corpus, corpusname)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

path = "./%s" % corpusname
mkdir_p(path)


for i in range(0, filelim):
    fid = corpus.fileids()[i]
    with open("%s/%s" % (path, fid), 'w') as out:
        # need to remove new lines here so MR interprets each file
        # as a single input
        out.write(corpus.raw(fid).replace('\n', ' '))
