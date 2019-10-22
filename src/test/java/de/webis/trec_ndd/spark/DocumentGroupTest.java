package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

public class DocumentGroupTest {
	@Test
	public void checkThatUnusedDocumentGroupIsDectedAsUnused() {
		HashSet<String> usedDocIDs = new HashSet<>();
		DocumentGroup group = new DocumentGroup()
				.setIds(new ArrayList<>(Arrays.asList("a", "b")));
		
		Assert.assertFalse(group.isUsed(usedDocIDs));
	}
	
	@Test
	public void checkThatUsedDocumentGroupIsDectedAsUsed() {
		HashSet<String> usedDocIDs = new HashSet<>(Arrays.asList("b"));
		DocumentGroup group = new DocumentGroup()
				.setIds(new ArrayList<>(Arrays.asList("a", "b")));
		
		Assert.assertTrue(group.isUsed(usedDocIDs));
	}
}
