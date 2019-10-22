package de.webis.trec_ndd.spark;

import java.util.Arrays;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

public class DocumentGroupJoinTest {
	@Test
	public void approveThatTwoDisjountGroupsAreAppended() {
		List<DocumentGroup> contentEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"a\",\"ids\":[\"a1\",\"a2\",\"a3\"]}"),
			new DocumentGroup("{\"hash\":\"b\",\"ids\":[\"b1\",\"b2\"]}")
		);
		List<DocumentGroup> retrievalEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"c\",\"ids\":[\"c1\",\"c3\"]}")
		);
		
		Approvals.verifyAsJson(DocumentGroup.appendRetrievalEquivalentToContentEquivalent(contentEquivalent, retrievalEquivalent));
	}
	
	@Test
	public void approveThatRetrievalEquivCanBeAppendedToFirstList() {
		List<DocumentGroup> contentEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"a\",\"ids\":[\"a1\",\"a2\",\"a3\"]}"),
			new DocumentGroup("{\"hash\":\"b\",\"ids\":[\"b1\",\"b2\"]}")
		);
		List<DocumentGroup> retrievalEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"c\",\"ids\":[\"c1\",\"c3\",\"a3\"]}")
		);
		
		Approvals.verifyAsJson(DocumentGroup.appendRetrievalEquivalentToContentEquivalent(contentEquivalent, retrievalEquivalent));
	}
	
	@Test
	public void approveThatRetrievalEquivCanBeAppendedToFirstListMultipleTimes() {
		List<DocumentGroup> contentEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"a\",\"ids\":[\"a1\",\"a2\",\"a3\"]}"),
			new DocumentGroup("{\"hash\":\"b\",\"ids\":[\"b1\",\"b2\"]}")
		);
		List<DocumentGroup> retrievalEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"c\",\"ids\":[\"c1\",\"c3\",\"a3\"]}"),
			new DocumentGroup("{\"hash\":\"c\",\"ids\":[\"c1\",\"c3\"]}"),
			new DocumentGroup("{\"hash\":\"c\",\"ids\":[\"d1\",\"a3\",\"b1\"]}")
		);
		
		Approvals.verifyAsJson(DocumentGroup.appendRetrievalEquivalentToContentEquivalent(contentEquivalent, retrievalEquivalent));
	}
}
