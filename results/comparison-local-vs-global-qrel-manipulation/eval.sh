#!/bin/bash -e

MEASURE="map"
TREC_EVAL="./trec_eval-linux-amd64"

echo -e "Ranking one has a class of relevant, content-equivalent documents on positions three, four, and five. Ranking two has a relevant document on position two. Ranking one is superior in terms of MAP on the original judgments.\n"

echo "RankingOne: $(${TREC_EVAL} -m ${MEASURE} qrel_original ranking_one_original)"
echo "RankingTwo: $(${TREC_EVAL} -m ${MEASURE} qrel_original ranking_two_original)"

echo -e "\n\nLocal judgment manipulation causes that ranking one is missing one document, while ranking two is missing three documents. The result is that Ranking one is superiour Ranking two. However, Ranking two should receive the larger score, since both rankings find one relevant document, but ranking two on a higher position!\n"

echo "RankingOne: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_one ranking_one_original)"
echo "RankingTwo: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_two ranking_two_original)"

echo -e "\n\nThis effect is removed if we apply the global-manipulation\n"

echo "RankingOne: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_one_original)"
echo "RankingTwo: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_two_original)"

