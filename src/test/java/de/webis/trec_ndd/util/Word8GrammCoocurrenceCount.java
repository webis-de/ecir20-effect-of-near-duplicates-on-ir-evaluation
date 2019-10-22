package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;

public class Word8GrammCoocurrenceCount {

	@Test
	public void checkThatWord8GrammWithoutIdsReturnsNoCoocurrence() {
		Word8GrammIndexEntry input = new Word8GrammIndexEntry()
				.setDocumentIds(null);
		List<Pair<Pair<String, String>, Integer>> expected = Collections.emptyList();
		List<Pair<Pair<String, String>, Integer>> actual = SymmetricPairUtil.extractCoocurrencePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatWord8GrammWithSingleIdReturnsNoCoocurrence() {
		Word8GrammIndexEntry input = new Word8GrammIndexEntry()
				.setDocumentIds(Arrays.asList("a"));
		List<Pair<Pair<String, String>, Integer>> expected = Collections.emptyList();
		List<Pair<Pair<String, String>, Integer>> actual = SymmetricPairUtil.extractCoocurrencePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatWord8GrammWithTwoIdsReturnsSingleCoocurrence() {
		Word8GrammIndexEntry input = new Word8GrammIndexEntry()
				.setDocumentIds(Arrays.asList("b", "a"));
		List<Pair<Pair<String, String>, Integer>> expected = Arrays.asList(pair("a", "b"));
		List<Pair<Pair<String, String>, Integer>> actual = SymmetricPairUtil.extractCoocurrencePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatWord8GrammWithFiveIdsReturnsManyCoocurrences() {
		Word8GrammIndexEntry input = new Word8GrammIndexEntry()
				.setDocumentIds(Arrays.asList("b", "a", "f", "e", "z"));
		List<Pair<Pair<String, String>, Integer>> expected = Arrays.asList(
				pair("a", "b"), pair("b", "f"), pair("b", "e"), pair("b", "z"),
				pair("a", "f"), pair("a", "e"), pair("a", "z"),
				pair("e", "f"), pair("f", "z"),
				pair("e", "z"));
		List<Pair<Pair<String, String>, Integer>> actual = SymmetricPairUtil.extractCoocurrencePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatCoocurrencePairsAreNotExtractedTwice() {
		Word8GrammIndexEntry input = new Word8GrammIndexEntry()
				.setDocumentIds(Arrays.asList("b", "a", "f", "a", "z"));
		List<Pair<Pair<String, String>, Integer>> expected = Arrays.asList(
				pair("a", "b"), pair("b", "f"), pair("b", "z"),
				pair("a", "f"), pair("a", "z"), pair("f", "z"));
		List<Pair<Pair<String, String>, Integer>> actual = SymmetricPairUtil.extractCoocurrencePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	private static Pair<Pair<String, String>, Integer> pair(String a, String b) {
		return Pair.of(Pair.of(a, b), 1);
	}
}
