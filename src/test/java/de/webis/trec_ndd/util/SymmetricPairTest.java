package de.webis.trec_ndd.util;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SymmetricPairTest {
	
	private static Pair<String, String> DUMMY_PAIR = Pair.of("a", "b");
	
	@Test
	public void checkNullPairCanBeCreated() {
		Pair<String, String> expected = Pair.of(null, null);
		Pair<String, String> actual = SymmetricPairUtil.of(null, null);
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
	
	@Test
	public void checkNullAndEmptyStringPairCanBeCreated() {
		Pair<String, String> expected = Pair.of(null, "");
		Pair<String, String> actual = SymmetricPairUtil.of(null, "");
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
	
	@Test
	public void checkEmptyStringAndNullPairCanBeCreated() {
		Pair<String, String> expected = Pair.of(null, "");
		Pair<String, String> actual = SymmetricPairUtil.of("", null);
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
	
	@Test
	public void checkCAndDPairCanBeCreated() {
		Pair<String, String> expected = Pair.of("c", "d");
		Pair<String, String> actual = SymmetricPairUtil.of("c", "d");
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
	
	@Test
	public void checkDAndCPairCanBeCreated() {
		Pair<String, String> expected = Pair.of("c", "d");
		Pair<String, String> actual = SymmetricPairUtil.of("d", "c");
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
	
	@Test
	public void checkCAndCPairCanBeCreated() {
		Pair<String, String> expected = Pair.of("c", "c");
		Pair<String, String> actual = SymmetricPairUtil.of("c", "c");
		
		Assert.assertEquals(expected, actual);
		Assert.assertNotEquals(actual, DUMMY_PAIR);
	}
}
