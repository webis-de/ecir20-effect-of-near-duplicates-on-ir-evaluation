package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class RunSortingTest {
	@Test
	public void testWithoutScoreTies() {
		List<Pair<String, Double>> initialRanking = Arrays.asList(
			Pair.of("a", 3.0),
			Pair.of("b", 1.0),
			Pair.of("c", 2.0)
		);
		List<String> expected = Arrays.asList("a", "c", "b");
		
		Assert.assertEquals(expected, sorted(initialRanking));
	}
	
	@Test
	public void testWithScoreTiesA() {
		List<Pair<String, Double>> initialRanking = Arrays.asList(
			Pair.of("a", 3.0),
			Pair.of("b", 3.0),
			Pair.of("c", 4.0)
		);
		List<String> expected = Arrays.asList("c", "a", "b");
		
		Assert.assertEquals(expected, sorted(initialRanking));
	}
	
	@Test
	public void testWithScoreTiesB() {
		List<Pair<String, Double>> initialRanking = Arrays.asList(
			Pair.of("d", 3.0),
			Pair.of("b", 3.0),
			Pair.of("c", 4.0)
		);
		List<String> expected = Arrays.asList("c", "b", "d");
		
		Assert.assertEquals(expected, sorted(initialRanking));
	}

	private static List<String> sorted(List<Pair<String, Double>> ranking) {
		return ScoringChanges.sortedStream(ranking)
				.map(Pair::getKey)
				.collect(Collectors.toList());
	}
}
