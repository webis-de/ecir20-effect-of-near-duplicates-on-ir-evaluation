#!/bin/bash -e

MEASURE="map"
TREC_EVAL="../../../../third-party/jtreceval/src/main/resources/trec_eval-linux-amd64"

echo -e "Ranking one has a relevant duplicate group on positions two, three, four and Ranking two has a relevant document on position one. Ranking one is superior in terms of MAP on the original judgments.\n"

echo "RankingOne: $(${TREC_EVAL} -m ${MEASURE} qrel_original ranking_one_original)"
echo "RankingTwo: $(${TREC_EVAL} -m ${MEASURE} qrel_original ranking_two_original)"

echo -e "\n\nRun-Local manipulation causes that ranking one is missing one document, while ranking two is missing three documents. The result is that RankingOne is superiour Ranking two. However, Ranking two should receive the larger score, since both rankings find one relevant document, but ranking two on a higher position!\n"

echo "RankingOne-Orig: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_one ranking_one_original)"
echo "RankingTwo-Orig: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_two ranking_two_original)"

echo "RankingOne-Duplicate-Free: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_one ranking_one_duplicate_free)"
echo "RankingTwo-Duplicate-Free: $(${TREC_EVAL} -m ${MEASURE} qrel_run_local_ranking_two ranking_two_duplicate_free)"

echo -e "\n\nThis effect is removed if we apply the global-manipulation\n"

echo "RankingOne-Orig: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_one_original)"
echo "RankingTwo-Orig: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_two_original)"

echo "RankingOne-Duplicate-Free: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_one_duplicate_free)"
echo "RankingTwo-Duplicate-Free: $(${TREC_EVAL} -m ${MEASURE} qrel_global ranking_two_duplicate_free)"


