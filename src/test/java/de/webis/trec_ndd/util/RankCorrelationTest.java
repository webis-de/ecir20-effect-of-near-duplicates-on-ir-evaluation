package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class RankCorrelationTest {
	@Test
	public void checkThatTwoIdenticalListsHaveCorrellationOf1() {
		List<String> a = Arrays.asList("A", "B", "C"); 
		List<String> b = Arrays.asList("A", "B", "C");
		double expected = 1.0;
		double actual = RankCorrelation.kendallTau(a, b);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void checkThatTwoInverseListsHaveCorrellationOfMinus1() {
		List<String> a = Arrays.asList("A", "B", "C"); 
		List<String> b = Arrays.asList("C", "B", "A");
		double expected = -1.0;
		double actual = RankCorrelation.kendallTau(a, b);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void checkThatTwoNearlyRandomListsHaveCorrellationOf() {
		List<String> a = Arrays.asList("A", "B", "C"); 
		List<String> b = Arrays.asList("B", "C", "A");
		double expected = -0.333333333;
		double actual = RankCorrelation.kendallTau(a, b);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void checkKendallTauWithBaezaYatesExample() {
		List<String> a = Arrays.asList("d_56", "d_123", "d_84", "d_8", "d_6");
		List<String> b = Arrays.asList("d_123", "d_84", "d_56", "d_6", "d_8");
		double expected = 0.4;
		double actual = RankCorrelation.kendallTau(a, b);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void checkKendallTauWithScoreTies() {
		List<Pair<String, Double>> a = Arrays.asList(
			Pair.of("a", 1.0),
			Pair.of("b", 1.0),
			Pair.of("c", 1.0)
		);
		List<Pair<String, Double>> b = Arrays.asList(
			Pair.of("c", 1.0),
			Pair.of("b", 1.0),
			Pair.of("a", 1.0)
		);
		double expected = 1;
		double actual = RankCorrelation.sortListsAndCalculateKendallTau(a, b);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testKendallTauAt5WithListsDivergentAfter5() {
		List<String> a = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
		List<String> b = Arrays.asList("A", "B", "C", "D", "E", "G", "F");
		
		double expected = 1;
		double actual = RankCorrelation.kendallTauAtK(a, b, 5);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testKendallTauAt5WithListsDivergentAtOneAndAfter5() {
		List<String> a = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "1", "2", "3", "4", "5", "6");
		List<String> b = Arrays.asList("B", "C", "D", "E", "G", "F", "A", "1", "2", "3", "4", "5", "6");
		
		double expected = 0.333333;
		double actual = RankCorrelation.kendallTauAtK(a, b, 5);

		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testKendallTauAt5WithListsDivergentAtOneAndAfter5AndSwitch() {
		List<String> a = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "1", "2");
		List<String> b = Arrays.asList("B", "C", "D", "E", "G", "A", "F", "1", "2");

		double expected = 0.333333;
		double actual = RankCorrelation.kendallTauAtK(a, b, 5);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testKendallTauAt5WithListsDivergentAtSecondAndAfter5() {
		List<String> a = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "1", "2", "3", "4", "5", "6");
		List<String> b = Arrays.asList("A", "C", "D", "E", "G", "F", "B", "1", "2", "3", "4", "5", "6");
		
		double expected = 0.466666;
		double actual = RankCorrelation.kendallTauAtK(a, b, 5);

		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testKendallTauAt5WithListsDivergentAtSecondAndAfter5AndSwitch() {
		List<String> a = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "1", "2");
		List<String> b = Arrays.asList("A", "C", "D", "E", "G", "B", "F", "1", "2");

		double expected = 0.4666666;
		double actual = RankCorrelation.kendallTauAtK(a, b, 5);
		
		Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testOriginalExample() {
		List<Pair<String, Double>> a = Arrays.asList(
				Pair.of("input.uwmtFmanual.gz", 0.7468),
				Pair.of("input.indri06AtdnD.gz", 0.7392),
				Pair.of("input.uogTB06S50L.gz", 0.7177),
				Pair.of("input.indri06AlceB.gz", 0.7046),
				Pair.of("input.uogTB06SSQL.gz", 0.7038)			
			);

			List<Pair<String, Double>> b = Arrays.asList(
				Pair.of("input.uwmtFmanual.gz", 0.7407),
				Pair.of("input.indri06AtdnD.gz", 0.7305),
				Pair.of("input.uogTB06S50L.gz", 0.7097),
				Pair.of("input.indri06AlceB.gz", 0.6999),
				Pair.of("input.TWTB06AD04.gz", 0.6966)
			);
			

			double expected = 0.8666666;
			double actual = RankCorrelation.sortedKendallTauAtK(a, b, 5);
			
			Assert.assertEquals(expected, actual, 1e-6);
	}
	
	@Test
	public void testOriginalExample2() {
		List<Pair<String, Double>> a = Arrays.asList(
				Pair.of("input.uwmtFmanual.gz", 0.7468),
				Pair.of("input.indri06AtdnD.gz", 0.7392),
				Pair.of("input.uogTB06S50L.gz", 0.7177),
				Pair.of("input.indri06AlceB.gz", 0.7046),
				Pair.of("input.uogTB06SSQL.gz", 0.7038),
				Pair.of("input.DCU05BASE.gz", 0.6093),
				Pair.of("input.zetabm.gz", 0.6082),
				Pair.of("input.hedge50.gz", 0.6008),
				Pair.of("input.mg4jAdhocV.gz", 0.5945),
				Pair.of("input.zetamerg2.gz", 0.5926)	
			);

			List<Pair<String, Double>> b = Arrays.asList(
				Pair.of("input.uwmtFmanual.gz", 0.7407),
				Pair.of("input.indri06AtdnD.gz", 0.7305),
				Pair.of("input.uogTB06S50L.gz", 0.7097),
				Pair.of("input.indri06AlceB.gz", 0.6999),
				Pair.of("input.TWTB06AD04.gz", 0.6966),
				Pair.of("input.zetabm.gz", 0.5811),
				Pair.of("input.DCU05BASE.gz", 0.5802),
				Pair.of("input.hedge50.gz", 0.5762),
				Pair.of("input.mg4jAdhocV.gz", 0.5679),
				Pair.of("input.zetamerg2.gz", 0.5634)
			);
			

			double expected = 0.56363636;
			double actual = RankCorrelation.sortedKendallTauAtK(a, b, 20);
			
			Assert.assertEquals(expected, actual, 1e-6);
	}
}
