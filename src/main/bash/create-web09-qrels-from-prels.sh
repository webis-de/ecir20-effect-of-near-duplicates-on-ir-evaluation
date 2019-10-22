#!/bin/bash -e

mkdir src/main/resources/topics-and-qrels/

# I am aware that prels should not be used as qrels (see http://ciir.cs.umass.edu/million/results07.html),
# but this seems to be the best way to calculate ndcg on the web track 2009
wget -O - https://raw.githubusercontent.com/castorini/anserini/master/src/main/resources/topics-and-qrels/prels.web.1-50.txt| awk '{print $1 " 0 " $2 " " $3}' > src/main/resources/topics-and-qrels/qrels.inofficial.web.1-50.txt
