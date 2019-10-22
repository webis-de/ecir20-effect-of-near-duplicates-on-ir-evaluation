#!/bin/bash -e


spark-submit \
	--class de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex \
	--deploy-mode cluster \
	--executor-cores 5\
	--executor-memory 25G\
	--driver-memory 25G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar\
	--documentSelection JUDGED\
	${@}

