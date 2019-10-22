#!/bin/bash -e

spark-submit \
	--class de.webis.trec_ndd.spark.SparkGroupByFingerprint \
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar \
	--documentSelection JUDGED \
	${@}

