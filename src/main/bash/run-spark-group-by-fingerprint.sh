#!/bin/bash -e

spark-submit \
	--deploy-mode cluster \
	--class de.webis.trec_ndd.spark.SparkGroupByFingerprint \
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar \
	--documentSelection JUDGED \
	${@}

