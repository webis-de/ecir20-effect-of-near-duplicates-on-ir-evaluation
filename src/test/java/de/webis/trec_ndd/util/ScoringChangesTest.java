package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ScoringChangesTest {
	
	@Test
	public void testCalculationOfScoringChangesOnEmptyList() {
		List<Pair<String, Double>> originalScores = Arrays.asList();
		List<Pair<String, Double>> manipulatedScores = Arrays.asList();
		List<Double> expected = Arrays.asList();
		
		ScoringChanges changes = new ScoringChanges(originalScores);
		List<Double> actual = changes.calculateScoringChangesTo(manipulatedScores);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatScoringChangesWithDuplicateEntriesFails() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0), Pair.of("a", 2.0));

		new ScoringChanges(originalScores);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatManipulatedScoresWithDuplicateEntriesFails() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("a", 2.0), Pair.of("a", 1.0));

		ScoringChanges changes = new ScoringChanges(originalScores);
		changes.calculateScoringChangesTo(manipulatedScores);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatScoringCalculationFailsOnDisjunctEntries() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("b", 1.0));

		ScoringChanges changes = new ScoringChanges(originalScores);
		changes.calculateScoringChangesTo(manipulatedScores);
	}
	
	@Test
	public void testCalculationOfScoringChangesOnSortedLists() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 6.0), Pair.of("b", 5.0),
			Pair.of("c", 4.0), Pair.of("d", 3.0),
			Pair.of("e", 2.0)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("e", 0.9), Pair.of("d", 0.8),
			Pair.of("c", 0.7), Pair.of("b", 0.6),
			Pair.of("a", 0.5)
		);
		double[] expected = new double[] {-5.5, -4.4, -3.3, -2.2, -1.10};
		
		ScoringChanges changes = new ScoringChanges(originalScores);
		double[] actual = changes.calculateScoringChangesTo(manipulatedScores)
				.stream().mapToDouble(i -> i)
				.toArray();

		Assert.assertArrayEquals(expected, actual,1e-6);
	}
	
	@Test
	public void testCalculationOfScoringChangesOnUnsortedLists() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("b", 3.0), Pair.of("a", 4.0),
			Pair.of("c", 1.0)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("b", 0.9), Pair.of("a", 5.4),
			Pair.of("c", 0.7)
		);
		double[] expected = new double[] {1.4, -2.1, -0.3};
			
		ScoringChanges changes = new ScoringChanges(originalScores);
		double[] actual = changes.calculateScoringChangesTo(manipulatedScores)
				.stream().mapToDouble(i -> i)
				.toArray();
			
		Assert.assertArrayEquals(expected, actual,1e-6);
	}
}
