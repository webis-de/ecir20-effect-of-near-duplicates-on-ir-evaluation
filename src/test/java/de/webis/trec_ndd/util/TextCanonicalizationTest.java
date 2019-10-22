package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TextCanonicalizationTest {
	@Test
	public void checkNullIsValidInputForFullCanonicalization() {
		List<String> expected = Arrays.asList();
		List<String> actual = TextCanonicalization.fullCanonicalization(null);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkEmptyStringIsValidInputForFullCanonicalization() {
		List<String> expected = Arrays.asList();
		List<String> actual = TextCanonicalization.fullCanonicalization("      ");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkStringWithoutStopwordsForFullCanonicalization() {
		List<String> expected = Arrays.asList("hallo", "world", "cat", "dog", "food");
		List<String> actual = TextCanonicalization.fullCanonicalization("Hallo World Cat  Dog   Food");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkStringWithStopwordsForFullCanonicalization() {
		List<String> expected = Arrays.asList("dog", "sai", "hello", "my", "mum");
		List<String> actual = TextCanonicalization.fullCanonicalization("The DoG is sayIng hello to my mum.");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkStringWithManyStemWordsForFullCanonicalization() {
		List<String> expected = Arrays.asList("sai", "run", "sayl", "comput", "plai", "game", "cool");
		List<String> actual = TextCanonicalization.fullCanonicalization("Saying, running, SayLing computering plays games cools.");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkNullIsValidInputForLevel5Canonicalization() {
		List<String> expected = Arrays.asList();
		List<String> actual = TextCanonicalization.level5Canonicalization(null);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkEmptyStringIsValidInputForLevel5Canonicalization() {
		List<String> expected = Arrays.asList();
		List<String> actual = TextCanonicalization.level5Canonicalization("      ");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkStringWithoutStopwordsForLevel5Canonicalization() {
		List<String> expected = Arrays.asList("hallo", "world", "cat", "dog", "food");
		List<String> actual = TextCanonicalization.level5Canonicalization("Hallo World Cat  Dog   Food");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkStringWithStopwordsForLevel5Canonicalization() {
		List<String> expected = Arrays.asList("dog", "saying", "hello", "my", "mum");
		List<String> actual = TextCanonicalization.level5Canonicalization("The DoG is sayIng hello to my mum.");
		
		Assert.assertEquals(expected, actual);
	}
}
