#!/bin/bash -e

PARALLELISM=70

spark-submit \
	--deploy-mode cluster \
	--class de.webis.trec_ndd.spark.SparkAnalyzeJudgedDocumentsTmp \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-memory 5G\
	--driver-memory 5G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar

