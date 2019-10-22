package de.webis.trec_ndd.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SystemRankingPreprocessingTest {

	private static List<Pair<String, Double>> RANKING_A = Arrays.asList(
		Pair.of("a", 3.0), Pair.of("b", 3.0), Pair.of("c", 3.0), Pair.of("d", 3.0)
	);
	
	private static List<Pair<String, Double>> RANKING_B = Arrays.asList(
		Pair.of("a", 3.0), Pair.of("b", 3.0), Pair.of("c", 3.0)
	);
	
	
	private static List<Pair<String, Double>> RANKING_C = Arrays.asList(
		Pair.of("a", 10.0), Pair.of("b", 9.0), Pair.of("c", 8.0),
		Pair.of("d", 7.0), Pair.of("e", 6.0), Pair.of("f", 5.0),
		Pair.of("g", 4.0), Pair.of("h", 3.0), Pair.of("i", 2.0)
	);
	
	@Test
	public void approveNames() {
		Assert.assertEquals("DO_NOTHING", SystemRankingPreprocessing.DO_NOTHING.name());
		Assert.assertEquals("DISCARD_LAST_PERCENT", SystemRankingPreprocessing.DISCARD_LAST_PERCENT.name());
	}
	
	@Test
	public void approveThatDoNothingPreprocessingDoesNotChangeRankingA() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DO_NOTHING;
		
		Assert.assertEquals(RANKING_A, rankingPreprocessing.preprocessOriginalRanking(RANKING_A));
	}
	
	@Test
	public void approveThatDiscard25PercentPreprocessingOnRankingA() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DISCARD_LAST_PERCENT;
		List<Pair<String, Double>> expected = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 3.0), Pair.of("c", 3.0)
		);
		Assert.assertEquals(expected, rankingPreprocessing.preprocessOriginalRanking(RANKING_A));
	}
	
	@Test
	public void approveThatDoNothingPreprocessingDoesNotChangeRankingB() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DO_NOTHING;
		
		Assert.assertEquals(RANKING_B, rankingPreprocessing.preprocessOriginalRanking(RANKING_B));
	}
	
	@Test
	public void approveThatDiscard25PercentPreprocessingOnRankingB() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DISCARD_LAST_PERCENT;
		
		Assert.assertEquals(RANKING_B, rankingPreprocessing.preprocessOriginalRanking(RANKING_B));
	}
	
	@Test
	public void approveThatDoNothingPreprocessingDoesNotChangeRankingC() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DO_NOTHING;
		
		Assert.assertEquals(RANKING_C, rankingPreprocessing.preprocessOriginalRanking(RANKING_C));
	}
	
	@Test
	public void approveThatDiscard25PercentPreprocessingOnRankingC() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DISCARD_LAST_PERCENT;
		List<Pair<String, Double>> expected = Arrays.asList(
			Pair.of("a", 10.0), Pair.of("b", 9.0), Pair.of("c", 8.0),
			Pair.of("d", 7.0), Pair.of("e", 6.0), Pair.of("f", 5.0),
			Pair.of("g", 4.0)
		);
		
		Assert.assertEquals(expected, rankingPreprocessing.preprocessOriginalRanking(RANKING_C));
	}
	
	@Test
	public void testThatAllElementsAreRemovedFromManipulatedList() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DISCARD_LAST_PERCENT;
		Set<String> ids = new HashSet<>();
		List<Pair<String, Double>> expected = new ArrayList<>();
		
		Assert.assertEquals(expected, rankingPreprocessing.preprocessManipulatedRanking(ids, RANKING_C));
		
	}
	
	@Test
	public void testThatSomeElementsAreRemovedFromManipulatedList() {
		SystemRankingPreprocessing rankingPreprocessing = SystemRankingPreprocessing.DISCARD_LAST_PERCENT;
		Set<String> ids = new HashSet<>(Arrays.asList("a", "e", "f", "h", "i"));
		List<Pair<String, Double>> expected = Arrays.asList(
				Pair.of("a", 10.0), Pair.of("e", 6.0), Pair.of("f", 5.0),
				Pair.of("h", 3.0), Pair.of("i", 2.0)
			);;
		
		Assert.assertEquals(expected, rankingPreprocessing.preprocessManipulatedRanking(ids, RANKING_C));
		
	}
}
