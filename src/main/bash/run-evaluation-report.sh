#!/bin/bash -e


spark-submit \
	--class de.webis.trec_ndd.spark.SparkDeduplicateAndEvaluate \
	--executor-cores 11\
	--executor-memory 15G\
	--driver-memory 25G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar \
	--similarity CANONICALIZED_MD5_CONTENT \
	--documentSelection JUDGED \
	${@}

