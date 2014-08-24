
source ./hadoop-streaming-env.sh

corpus=$1

output=/sampa/home/bdmyers/nltk-apps/out
rm -rf $output

$HADOOP_HOME/bin/hadoop  jar $HADOOP_HOME/$RELATIVE_PATH_JAR \
    -D mapred.reduce.tasks=0 \
    -verbose \
    -input /sampa/home/bdmyers/nltk-apps/$1 \
    -output $output \
    -mapper /sampa/home/bdmyers/nltk-apps/tagger_map.py 
