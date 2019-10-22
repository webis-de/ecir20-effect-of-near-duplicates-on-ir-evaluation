#!/bin/bash -e

PARALLELISM=70

#hdfs dfs -rm -f -R trec-ndd-test-hashing

spark-submit \
	--class de.webis.trec_ndd.spark.SparkDeduplicateAndEvaluate \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-cores 11\
	--executor-memory 15G\
	--driver-memory 25G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

