package de.webis.trec_ndd.util;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

public class ScoringChanges {
	private final List<Pair<String, Double>> originalScores;
	
	public ScoringChanges(List<Pair<String, Double>> originalScores) {
		failIfScoresContainsDuplicatedRuns(originalScores);
		this.originalScores = originalScores;
	}

	static void failIfScoresContainsDuplicatedRuns(List<Pair<String, Double>> scoredRuns) {
		Set<String> runs = extractUniqueRuns(scoredRuns);
		
		if(runs.size() != scoredRuns.size()) {
			throw new RuntimeException("");
		}
	}

	void failIfUnknownElementsAreInRuns(List<Pair<String, Double>> manipulatedScores) {
		failIfUnknownElementsAreInRuns(originalScores, manipulatedScores);
	}
	
	static void failIfUnknownElementsAreInRuns(List<Pair<String, Double>> first, List<Pair<String, Double>> second) {
		Set<String> a = extractUniqueRuns(first);
		Set<String> b = extractUniqueRuns(second);
		
		if(!a.equals(b)) {
			throw new RuntimeException("");
		}
	}

	private static Set<String> extractUniqueRuns(List<Pair<String, Double>> scoredRuns) {
		return scoredRuns.stream()
				.map(Pair::getLeft)
				.collect(Collectors.toSet());
	}
	
	public List<Double> calculateScoringChangesTo(List<Pair<String, Double>> manipulatedScores) {
		failIfUnknownElementsAreInRuns(manipulatedScores);
		Map<String, Double> l = rankingAsMap(manipulatedScores);
		
		return sortedStream(originalScores)
				.map(i -> l.get(i.getKey()) - i.getRight())
				.collect(Collectors.toList());
	}

	public static Map<String, Double> rankingAsMap(List<Pair<String, Double>> ranking) {
		failIfScoresContainsDuplicatedRuns(ranking);
		Map<String, Double> ret = new LinkedHashMap<>();
		
		sortedStream(ranking)
			.forEach(i -> ret.put(i.getLeft(), i.getRight()));
		
		return ret;
	}
	
	public static List<Pair<String, Double>> mapToRanking(Map<String, Double> m) {
		return m.entrySet().stream()
				.map(e -> Pair.of(e.getKey(), e.getValue()))
				.collect(Collectors.toList());
	}
	
	static Stream<Pair<String, Double>> sortedStream(List<Pair<String, Double>> ret) {
		return ret.stream()
				.sorted(trecStyleComperator());
	}
	
	public static Comparator<? super Pair<String, Double>> trecStyleComperator() {
		return (a,b) -> {
			int ret = b.getRight().compareTo(a.getRight());
			
			return ret != 0.0 ? ret : a.getLeft().compareTo(b.getLeft());
		};
	}
}
