package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ParticipationModelTest {

	@Test
	public void testParticipationModelsOnEmptyList() {
		List<Pair<String, Double>> originalScores = Arrays.asList();
		List<Pair<String, Double>> manipulatedScores = Arrays.asList();
		List<Integer> expected = Arrays.asList();
		
		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateCheatingVector(manipulatedScores));
		Assert.assertEquals(expected, participationModel.calculateIdealVector(manipulatedScores));
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatParticipationModelWithDuplicateEntriesFails() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0), Pair.of("a", 2.0));

		new ParticipationModel(originalScores);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatCheatingVectorWithDuplicateEntriesFails() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("a", 2.0), Pair.of("a", 1.0));

		ParticipationModel model = new ParticipationModel(originalScores);
		model.calculateCheatingVector(manipulatedScores);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatIdealVectorWithDuplicateEntriesFails() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("a", 2.0), Pair.of("a", 1.0));

		ParticipationModel model = new ParticipationModel(originalScores);
		model.calculateIdealVector(manipulatedScores);
	}
	
	@Test(expected = RuntimeException.class)
	public void testThatCheatingVectorFailsOnDisjunctEntries() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("b", 1.0));

		ParticipationModel model = new ParticipationModel(originalScores);
		model.calculateCheatingVector(manipulatedScores);
	}

	@Test(expected = RuntimeException.class)
	public void testThatIdealVectorFailsOnDisjunctEntries() {
		List<Pair<String, Double>> originalScores = Arrays.asList(Pair.of("a", 1.0));
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(Pair.of("b", 1.0));

		ParticipationModel model = new ParticipationModel(originalScores);
		model.calculateIdealVector(manipulatedScores);
	}
	
	@Test
	public void testCheatingVectorOnExampleWhereLastRunImprovesTwoPositions() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 2.0), Pair.of("c", 1.0001)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 1.0), Pair.of("b", 1.5), Pair.of("c", 1.1)
		);
		List<Integer> expected = Arrays.asList(0, 0, 2);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateCheatingVector(manipulatedScores));
	}

	@Test
	public void testCheatingVectorOnExampleWhereSecondRunImprovesOnePosition() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 1.05), Pair.of("b", 2.0), Pair.of("c", 1.6)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 1.0), Pair.of("b", 1.5), Pair.of("c", 1.1)
		);
		List<Integer> expected = Arrays.asList(0, 1, 0);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateCheatingVector(manipulatedScores));
	}
	
	@Test
	public void testCheatingVectorOnExampleWhereNoChangesApply() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 4.0), Pair.of("b", 2.5), Pair.of("c", 1.5)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 2.0), Pair.of("c", 1.0)
		);
		List<Integer> expected = Arrays.asList(0, 0, 0);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateCheatingVector(manipulatedScores));
	}
	
	@Test
	public void testIdealVectorOnExampleWhereFirstRunLosesTwoPositions() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 2.0), Pair.of("c", 1.0001)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 1.0), Pair.of("b", 1.5), Pair.of("c", 0.9)
		);
		List<Integer> expected = Arrays.asList(-2, 0, 0);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateIdealVector(manipulatedScores));
	}
	
	@Test
	public void testIdealVectorOnExampleWhereSecondRunLosesOnePosition() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 2.0), Pair.of("c", 1.0001)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 2.5), Pair.of("b", 1.0), Pair.of("c", 0.9)
		);
		List<Integer> expected = Arrays.asList(0, -1, 0);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateIdealVector(manipulatedScores));
	}
	
	@Test
	public void testIdealVectorOnExampleWithoutChanges() {
		List<Pair<String, Double>> originalScores = Arrays.asList(
			Pair.of("a", 3.0), Pair.of("b", 2.0), Pair.of("c", 1.0001)
		);
		List<Pair<String, Double>> manipulatedScores = Arrays.asList(
			Pair.of("a", 2.5), Pair.of("b", 1.2), Pair.of("c", 0.5)
		);
		List<Integer> expected = Arrays.asList(0, 0, 0);

		ParticipationModel participationModel = new ParticipationModel(originalScores);
		
		Assert.assertEquals(expected, participationModel.calculateIdealVector(manipulatedScores));
	}
}
