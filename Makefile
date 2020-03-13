NAMESPACE="wstud-thesis-bittner"
ES_NAME="${NAMESPACE}-elasticsearch"
KIBANA_NAME="${NAMESPACE}-kibana"


hash-datasets-gov: install
	hdfs dfs -rm -r -f trec-ndd-hashes-gov1 &&\
	hdfs dfs -rm -r -f trec-ndd-hashes-gov2 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c GOV1 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c GOV2

copy-run-docs-gov: install
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c GOV1 &&\
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c GOV2

deduplicate-gov: install
	hdfs dfs -rm -r -f trec-fingerprint-groups-gov1 &&\
	hdfs dfs -rm -r -f trec-fingerprint-groups-gov2 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c GOV1 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c GOV2

calculate-s3-gov: install
	./src/main/bash/calculate-s3-with-spex.sh -c GOV2 --threshold 0.68

calculate-s3-million-query: install
	./src/main/bash/calculate-s3-with-spex.sh -c GOV2 --threshold 0.68

calculate-s3-CLUEWEB09: install
	./src/main/bash/calculate-s3-with-spex.sh -c CLUEWEB09 --threshold 0.84

calculate-s3-CLUEWEB12: install
	./src/main/bash/calculate-s3-with-spex.sh -c CLUEWEB12 --threshold 0.84

calculate-s3-core: install
	./src/main/bash/calculate-s3-with-spex.sh -c CORE2017 --threshold 0.68 &&\
	./src/main/bash/calculate-s3-with-spex.sh -c CORE2018 --threshold 0.68

index-8-gramms-gov: install
	./src/main/bash/run-8-gramm-indexing.sh -c GOV1 --chunkSelection SPEX &&\
	./src/main/bash/run-8-gramm-indexing.sh -c GOV2 --chunkSelection SPEX

index-8-gramms-million-query: install
	./src/main/bash/run-8-gramm-indexing.sh -c GOV2_MQ --chunkSelection SPEX

hash-datasets-common-core: install
	hdfs dfs -rm -r -f trec-ndd-hashes-core2017 &&\
	hdfs dfs -rm -r -f trec-ndd-hashes-core2018 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c CORE2017 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c CORE2018

sample-pairs-to-judge-on-terabyte2004: install
	./src/main/bash/create-sample-documents-to-judge.sh --collections GOV2

deduplicate-common-core: install
	hdfs dfs -rm -r -f trec-fingerprint-groups-core2017 &&\
	hdfs dfs -rm -r -f trec-fingerprint-groups-core2018 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c CORE2017 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c CORE2018

index-8-gramms-common-core: install 
	./src/main/bash/run-8-gramm-indexing.sh -c CORE2017 --chunkSelection SPEX &&\
	./src/main/bash/run-8-gramm-indexing.sh -c CORE2018 --chunkSelection SPEX

copy-run-docs-common-core: install
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c CORE2017 &&\
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c CORE2018

hash-dataset-clueweb09: install
	hdfs dfs -rm -r -f trec-ndd-hashes-clueweb09 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c CLUEWEB09

deduplicate-clueweb09: install
	hdfs dfs -rm -r -f trec-fingerprint-groups-clueweb09 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c CLUEWEB09

index-8-gramms-clueweb09: install
	./src/main/bash/run-8-gramm-indexing.sh -c CLUEWEB09 --chunkSelection SPEX

copy-run-docs-clueweb09: install
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c CLUEWEB09

hash-dataset-clueweb12: install
	hdfs dfs -rm -r -f trec-ndd-hashes-clueweb12 &&\
	./src/main/bash/run-spark-hash-dataset.sh -c CLUEWEB12

deduplicate-clueweb12: install
	hdfs dfs -rm -r -f trec-fingerprint-groups-clueweb12 &&\
	./src/main/bash/run-spark-group-by-fingerprint.sh -c CLUEWEB12

copy-run-docs-clueweb12: install
	./src/main/bash/copy-run-file-docs-to-hdfs.sh -c CLUEWEB12

index-8-gramms-clueweb12: install
	./src/main/bash/run-8-gramm-indexing.sh -c CLUEWEB12 --chunkSelection SPEX

run: install
	java -jar target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar

run-local-experiment-evaluation: install
	cd  ../wstud-thesis-reimer/source/tasks/ &&\
	sudo java -cp ../../../trec-near-duplicates/target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar de.webis.trec_ndd.local.BM25BaselineRanking &&\
	make run-preparation-for-analysis-on-experiments &&\
	cd ../../../trec-near-duplicates &&\
	java -cp target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar de.webis.trec_ndd.local.App

evaluate-judgment-inconsistencies: install
	./src/main/bash/evaluate-judgment-inconsistencies.sh --collections CLUEWEB09 CLUEWEB12 --threshold 0.84 |tee intermediate-results/judgment-inconsistencies-clueweb.jsonl &&\
	./src/main/bash/evaluate-judgment-inconsistencies.sh --collections GOV2 --threshold 0.68 |tee intermediate-results/judgment-inconsistencies-gov.jsonl &&\
	./src/main/bash/evaluate-judgment-inconsistencies.sh --collections CORE2017 CORE2018 --threshold 0.68 |tee intermediate-results/judgment-inconsistencies-core.jsonl

#delete-k8s-environment:
#    bash -c 'helm delete ${ES_NAME} --purge || echo "${ES_NAME} is already deleted"' &&\
#    bash -c 'helm delete ${KIBANA_NAME} --purge || echo "${KIBANA_NAME} is already deleted"'

install-k8s-environment:
	helm repo add elastic https://helm.elastic.co &&\
	helm repo update &&\
	helm install    --namespace=${NAMESPACE} \
		--replace \
		--name ${ES_NAME} \
		--set replicas=1,minimumMasterNodes=1,service.type=NodePort,volumeClaimTemplate.resources.requests.storage=3Gi \
		elastic/elasticsearch --version 7.2.0 &&\
	helm install    --namespace=${NAMESPACE} \
		--replace \
		--name ${KIBANA_NAME} \
		--set service.type=NodePort \
		elastic/kibana

evaluate-gov: install
	./src/main/bash/run-evaluation-report.sh -c GOV2 --threshold 0.68 |tee results/gov-evaluation.jsonl

evaluate-core2018: install
	./src/main/bash/run-evaluation-report.sh -c CORE2018 --threshold 0.68|tee results/core2018-evaluation.jsonl

evaluate-core2017: install
	./src/main/bash/run-evaluation-report.sh -c CORE2017 --threshold 0.68|tee results/core2017-evaluation.jsonl

evaluate-clueweb09: install
	./src/main/bash/run-evaluation-report.sh -c CLUEWEB09 --threshold 0.84|tee results/clueweb09-evaluation.jsonl

evaluate-clueweb12: install
	./src/main/bash/run-evaluation-report.sh -c CLUEWEB12 --threshold 0.84|tee results/clueweb12-evaluation.jsonl

install: install-third-party
	./mvnw clean install -DskipTests

docker-bash: build-docker-image
	docker run --rm -ti -v /mnt/nfs/webis20/:/mnt/nfs/webis20/ -v ${PWD}/results:/trec-ndd/results --entrypoint /bin/bash trec-ndd-kibi9872:0.0.1

build-docker-image: checkout-submodules
	cd  third-party/private-webis-datascience-image/docker/ &&\
	make build &&\
	cd ../../.. &&\
	docker build --tag trec-ndd-kibi9872:0.0.1 .

clean-stuff:
	rm -Rf /tmp/tmp-qrel* &&\
	rm -Rf /tmp/qrels* &&\
	rm -Rf /tmp/tmp-unzipped* &&\
	rm -Rf /tmp/.run*

install-third-party: checkout-submodules
	echo "currently no third-party :)"

checkout-submodules:
	git submodule update --init --recursive

