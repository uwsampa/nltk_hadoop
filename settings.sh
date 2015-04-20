#!/usr/bin/env bash

export HADOOP_VERSION=2.6.0
export AVRO_VERSION=1.7.4
export HADOOP_HOME=/shared/hadoop/current
export NLTK_HOME=/shared/patents/nltk-hadoop
export HADOOP_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar
export AVRO_JAR=$HADOOP_HOME/share/hadoop/common/lib/avro-$AVRO_VERSION.jar
