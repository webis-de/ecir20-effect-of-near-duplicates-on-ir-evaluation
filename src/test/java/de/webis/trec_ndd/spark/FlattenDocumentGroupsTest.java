package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;

public class FlattenDocumentGroupsTest {
	@Test
	public void testOnDocumentGroupWithoutEntry() {
		DocumentGroup docGroup = new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[]}");
		List<S3ScoreIntermediateResult> expected = new ArrayList<>();
		List<S3ScoreIntermediateResult> actual = flattenGroup(docGroup);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testOnDocumentGroupWithOneEntry() {
		DocumentGroup docGroup = new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"a\"]}");
		List<S3ScoreIntermediateResult> expected = new ArrayList<>();
		List<S3ScoreIntermediateResult> actual = flattenGroup(docGroup);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallDocumentGroup() {
		DocumentGroup docGroup = new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"a\", \"b\"]}");
		List<S3ScoreIntermediateResult> actual = flattenGroup(docGroup);
		
		//a<->b
		Approvals.verify(actual);
	}
	
	@Test
	public void approveLargeDocumentGroup() {
		DocumentGroup docGroup = new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":["
				+ "\"c\", \"h\", \"a\","
				+ "\"b\", \"e\"]}");
		List<S3ScoreIntermediateResult> actual = flattenGroup(docGroup);
		
		// a - b, a - c, a - e, a-h
		// b - c, b - h, b - e
		// c - e, c - h
		// h - e
		Approvals.verify(actual);
	}
	
	//{\"hash\":\"FooBar\",\"ids\":[\"g_1_1\",\"g_1_2\",\"g_1_3\"]}
	List<S3ScoreIntermediateResult> flattenGroup(DocumentGroup group) {
		return Lists.newArrayList(S3ScoreOnWord8GrammIndex.shortRetrievalEquivalentDocGroupToS3IntermediateResults(group));
	}
}
