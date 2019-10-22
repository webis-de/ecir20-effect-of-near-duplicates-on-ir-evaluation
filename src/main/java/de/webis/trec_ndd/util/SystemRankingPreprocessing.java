package de.webis.trec_ndd.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import lombok.AllArgsConstructor;

public interface SystemRankingPreprocessing extends Serializable {

	public static final SystemRankingPreprocessing DO_NOTHING = PredefinedPreprocessings.DO_NOTHING;
	
	public static final SystemRankingPreprocessing DISCARD_LAST_PERCENT = PredefinedPreprocessings.DISCARD_LAST_PERCENT;

	public static final List<SystemRankingPreprocessing> ALL = Collections.unmodifiableList(Arrays.asList(
		DO_NOTHING, DISCARD_LAST_PERCENT
	));

	public List<Pair<String, Double>> preprocessOriginalRanking(List<Pair<String, Double>> ranking);
	
	public String name();
	
	public default List<Pair<String, Double>> preprocessManipulatedRanking(Set<String> idsToKeep, List<Pair<String, Double>> manipulatedRanking) {
		return manipulatedRanking.stream()
				.filter(i -> idsToKeep.contains(i.getKey()))
				.sorted(ScoringChanges.trecStyleComperator())
				.collect(Collectors.toList());
	}
	
	@AllArgsConstructor
	public enum PredefinedPreprocessings implements SystemRankingPreprocessing {
		DO_NOTHING(i -> i),
		DISCARD_LAST_PERCENT(ranking -> {
			int returnSize = (int)(Math.ceil(((double)ranking.size()) - (0.25* (double) ranking.size())));
			return ranking.stream()
					.sorted(ScoringChanges.trecStyleComperator())
					.limit(returnSize)
					.collect(Collectors.toList());
		});
		
		private final Function<List<Pair<String, Double>>, List<Pair<String, Double>>> v;

		@Override
		public List<Pair<String, Double>> preprocessOriginalRanking(List<Pair<String, Double>> ranking) {
			return v.apply(ranking);
		}
	}
}
