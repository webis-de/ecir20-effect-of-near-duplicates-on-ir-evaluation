package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class AverageRunMeasureTest {
	@Test
	public void testAverageMeasureOnExampleWithPositiveDifference() {
		List<Pair<String, Double>> original = Arrays.asList(
			Pair.of("a", 4.0),
			Pair.of("b", 2.0)
		);
		List<Pair<String, Double>> manipulated = Arrays.asList(
				Pair.of("a", 2.0),
				Pair.of("b", 1.0)
			);
		
		AverageRunMeasure avgMeasure = new AverageRunMeasure(original);
		
		Map<String, Double> expected = ImmutableMap.<String, Double>builder()
				.put("avgOriginal", 3.0)
				.put("avgManipulated", 1.5)
				.put("difference", 1.5)
				.build();
		
		Assert.assertEquals(expected, avgMeasure.reportAvg(manipulated));
	}
	
	@Test
	public void testAverageMeasureOnExampleWithNeutralDifference() {
		List<Pair<String, Double>> original = Arrays.asList(
			Pair.of("a", 4.0),
			Pair.of("b", 2.0)
		);
		List<Pair<String, Double>> manipulated = Arrays.asList(
				Pair.of("a", 4.0),
				Pair.of("b", 2.0)
			);
		
		AverageRunMeasure avgMeasure = new AverageRunMeasure(original);
		
		Map<String, Double> expected = ImmutableMap.<String, Double>builder()
				.put("avgOriginal", 3.0)
				.put("avgManipulated", 3.0)
				.put("difference", 0.0)
				.build();
		
		Assert.assertEquals(expected, avgMeasure.reportAvg(manipulated));
	}
	
	@Test
	public void testAverageMeasureOnExampleWithNegativeDifference() {
		List<Pair<String, Double>> original = Arrays.asList(
			Pair.of("a", 4.0),
			Pair.of("b", 2.0),
			Pair.of("c", 2.0),
			Pair.of("d", 4.0)
		);
		List<Pair<String, Double>> manipulated = Arrays.asList(
				Pair.of("a", 8.0),
				Pair.of("b", 4.0),
				Pair.of("c", 4.0),
				Pair.of("d", 8.0)
			);
		
		AverageRunMeasure avgMeasure = new AverageRunMeasure(original);
		
		Map<String, Double> expected = ImmutableMap.<String, Double>builder()
				.put("avgOriginal", 3.0)
				.put("avgManipulated", 6.0)
				.put("difference", -3.0)
				.build();
		
		Assert.assertEquals(expected, avgMeasure.reportAvg(manipulated));
	}
}
