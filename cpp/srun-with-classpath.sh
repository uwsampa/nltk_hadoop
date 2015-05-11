#!/bin/bash

CLASSPATH=$(for i in $(hadoop classpath | tr : '\n' | grep -v jar | sed 's/\/\*$//'); do ls $i/*.jar; done | tr '\n' ':')

srun -- $*
