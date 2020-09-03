#!/bin/bash -e

PARALLELISM=70
./mvnw clean install -DskipTests

spark-submit \
	--deploy-mode cluster \
	--class de.webis.trec_ndd.spark.SparkHashDataset \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-cores 11\
	--executor-memory 15G\
	--driver-memory 25G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar

