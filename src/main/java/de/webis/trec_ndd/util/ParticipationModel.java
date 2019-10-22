package de.webis.trec_ndd.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import static de.webis.trec_ndd.util.ScoringChanges.failIfScoresContainsDuplicatedRuns;
import static de.webis.trec_ndd.util.ScoringChanges.failIfUnknownElementsAreInRuns;
import static de.webis.trec_ndd.util.ScoringChanges.sortedStream;

public class ParticipationModel {
	private final List<Pair<String, Double>> originalScores;
	
	public ParticipationModel(List<Pair<String, Double>> originalScores) {
		failIfScoresContainsDuplicatedRuns(originalScores);
		this.originalScores = originalScores;
	}
	public List<Integer> calculateCheatingVector(List<Pair<String, Double>> manipulatedScores) {
		failIfScoresContainsDuplicatedRuns(manipulatedScores);
		failIfUnknownElementsAreInRuns(originalScores, manipulatedScores);
		
		return sortedStream(manipulatedScores)
				.map(i -> calculateCheatingScoreForEntry(i, manipulatedScores))
				.collect(Collectors.toList());
	}
	
	public List<Integer> calculateIdealVector(List<Pair<String, Double>> manipulatedScores) {
		failIfScoresContainsDuplicatedRuns(manipulatedScores);
		failIfUnknownElementsAreInRuns(originalScores, manipulatedScores);
		
		return sortedStream(originalScores)
				.map(i -> calculateIdealScoreForEntry(extractRunWithName(i.getKey(), manipulatedScores), originalScores))
				.collect(Collectors.toList());
	}

	private Integer calculateCheatingScoreForEntry(Pair<String, Double> entry, List<Pair<String, Double>> manipulatedScores) {
		String cheater = entry.getKey();
		
		Map<String, Integer> baselinePositions = runToPosition(manipulatedScores);
		List<Pair<String, Double>> runsWithCheater = new LinkedList<>(manipulatedScores).stream()
				.filter(i -> !cheater.equals(i.getKey()))
				.collect(Collectors.toList());
		runsWithCheater.add(cheaterScore(cheater));
		
		Map<String, Integer> positionsWithSingleCheater = runToPosition(runsWithCheater);
		
		return baselinePositions.get(cheater) - positionsWithSingleCheater.get(cheater);
	}
	
	private Integer calculateIdealScoreForEntry(Pair<String, Double> entry, List<Pair<String, Double>> originalScores) {
		String ideal = entry.getKey();
		Map<String, Integer> baselinePositions = runToPosition(originalScores);
		List<Pair<String, Double>> runsWithIdeal = new LinkedList<>(originalScores).stream()
				.filter(i -> !ideal.equals(i.getKey()))
				.collect(Collectors.toList());
		runsWithIdeal.add(entry);

		Map<String, Integer> positionsWithSingleIdeal = runToPosition(runsWithIdeal);
		
		return baselinePositions.get(ideal) - positionsWithSingleIdeal.get(ideal); 
	}
	
	private Pair<String, Double> cheaterScore(String cheater) {
		return extractRunWithName(cheater, originalScores);
	}
	
	private static Pair<String, Double> extractRunWithName(String runName, List<Pair<String, Double>> runs) {
		List<Pair<String, Double>> ret = runs.stream()
				.filter(i -> runName.equals(i.getKey()))
				.collect(Collectors.toList());
		
		if(ret.size() != 1) {
			throw new RuntimeException("");
		}
		
		return ret.get(0);
	}
	
	private static Map<String, Integer> runToPosition(List<Pair<String, Double>> runs) {
		failIfScoresContainsDuplicatedRuns(runs);
		List<String> ranking = sortedStream(runs)
				.map(Pair::getLeft)
				.collect(Collectors.toList());
		
		return IntStream.range(0, ranking.size())
				.mapToObj(i -> Pair.of(ranking.get(i), i))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue));
	}
}
