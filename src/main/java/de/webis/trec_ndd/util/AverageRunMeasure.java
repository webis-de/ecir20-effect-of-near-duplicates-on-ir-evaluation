package de.webis.trec_ndd.util;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class AverageRunMeasure {
	private final List<Pair<String, Double>> originalRanking;

	public Map<String, Double> reportAvg(List<Pair<String, Double>> ranking) {
		double origAvg = avg(originalRanking);
		double newAvg = avg(ranking);
		
		return ImmutableMap.<String, Double>builder()
			.put("avgOriginal", origAvg)
			.put("avgManipulated", newAvg)
			.put("difference", origAvg - newAvg)
			.build();
	}

	private static final double avg(List<Pair<String, Double>> ranking) {
		return ranking.stream()
			.mapToDouble(Pair::getRight)
			.average().getAsDouble();
	}
}
