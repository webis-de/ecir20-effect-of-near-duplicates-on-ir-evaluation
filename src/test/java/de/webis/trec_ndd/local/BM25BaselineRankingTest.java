package de.webis.trec_ndd.local;

import java.io.InputStream;

import org.approvaltests.Approvals;
import org.junit.Test;

public class BM25BaselineRankingTest {
	
	private static final String SMALL_TRAIN_TEST_SPLIT = "{\"name\": \"foo-bar\", \"train\": [], \"test\": [\"110\"]}";
	private static final String MEDIUM_TRAIN_TEST_SPLIT = "{\"name\": \"foo-bar\", \"train\": [], \"test\": [\"110\", \"abcd\", \"hello-world\"]}";
	
	@Test
	public void approveBM25RankingForSmallExampleFeatureVectorFile() {
		InputStream is = BM25BaselineRankingTest.class.getResourceAsStream("/small-example-feature-vector-file.json");
		String ranking = BM25BaselineRanking.featureVectorsToRunFile(is, SMALL_TRAIN_TEST_SPLIT);
		
		Approvals.verify(ranking);
	}
	
	@Test
	public void approveBM25RankingForMediumExampleFeatureVectorFile() {
		InputStream is = BM25BaselineRankingTest.class.getResourceAsStream("/medium-example-feature-vector-file.json");
		String ranking = BM25BaselineRanking.featureVectorsToRunFile(is, MEDIUM_TRAIN_TEST_SPLIT);
		
		Approvals.verify(ranking);
	}
	
	@Test
	public void approveBM25RankingForMediumExampleFeatureVectorFileOnSmallTrainTestSplit() {
		InputStream is = BM25BaselineRankingTest.class.getResourceAsStream("/medium-example-feature-vector-file.json");
		String ranking = BM25BaselineRanking.featureVectorsToRunFile(is, SMALL_TRAIN_TEST_SPLIT);
		
		Approvals.verify(ranking);
	}
}
