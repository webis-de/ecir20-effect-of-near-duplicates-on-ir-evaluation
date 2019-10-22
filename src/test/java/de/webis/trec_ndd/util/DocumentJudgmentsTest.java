package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.DocumentJudgments;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.trec_collections.SharedTask;

public class DocumentJudgmentsTest {

	@Test
	public void testTopicsAreReturnedOnTestExample() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentJudgments judgments = task.documentJudgments();
		List<String> topics = judgments.topics();
		List<String> expectedTopics = Arrays.asList("307", "310", "321", "325", "330", "336", "341", "344", "345", "347", "350", "353", "354", "355", "356", "362", "363", "367", "372", "375", "378", "379", "389", "393", "394", "397", "399", "400", "404", "408", "414", "416", "419", "422", "423", "426", "427", "433", "435", "436", "439", "442", "443", "445", "614", "620", "626", "646", "677", "690");
		
		Assert.assertEquals(50, topics.size());
		
		for(String topic : expectedTopics) {
			Assert.assertTrue(topics.contains(topic));
		}
	}
	
	@Test
	public void testCountOfJudgmentsIsValid() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentJudgments judgments = task.documentJudgments();

		int expected = 30030;
		int actual = judgments.judgmentCount();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testDocumentJudgmentsOnTestExample() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentJudgments judgments = task.documentJudgments();
		
		Assert.assertEquals("2", judgments.labelForTopicAndDocument("307", "1041188"));
		Assert.assertEquals("0", judgments.labelForTopicAndDocument("307", "1121368"));
		Assert.assertEquals("1", judgments.labelForTopicAndDocument("307", "1093334"));
		Assert.assertEquals("UNKNOWN", judgments.labelForTopicAndDocument("307", "BLA"));
		
		Assert.assertEquals("1", judgments.labelForTopicAndDocument("379", "552301"));
		Assert.assertEquals("0", judgments.labelForTopicAndDocument("379", "552901"));
		Assert.assertEquals("0", judgments.labelForTopicAndDocument("379", "431528"));
		Assert.assertEquals("0", judgments.labelForTopicAndDocument("379", "366626"));
		
		
		
		Assert.assertEquals("UNKNOWN", judgments.labelForTopicAndDocument("379", "BLA"));
	}
	
	@Test
	public void testThatAll0GroupGetsUniqueLabels() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"431528\", \"366626\"]}");
		Set<String> expected = new HashSet<>(Arrays.asList("0"));
		Set<String> actual = task.documentJudgments().labelsInGroupForTopic("379", docGroup);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatAllWithTwoLabels() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"431528\", \"366626\", \"ADASDAS\"]}");
		Set<String> expected = new HashSet<>(Arrays.asList("0", "UNKNOWN"));
		Set<String> actual = task.documentJudgments().labelsInGroupForTopic("379", docGroup);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatAllWithUnknownLabel() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"431528\", \"366626\", \"ADASDAS\"]}");
		Set<String> expected = new HashSet<>(Arrays.asList("UNKNOWN"));
		Set<String> actual = task.documentJudgments().labelsInGroupForTopic("307", docGroup);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatSmallGroupWithUnknownLabelsHasNoInconsistency() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"ADASDAS1\", \"ADASDAS2\", \"ADASDAS3\", \"ADASDAS4\"]}");
		
		Assert.assertEquals(Boolean.FALSE, task.documentJudgments().groupHasInconsistency(docGroup));
		Assert.assertEquals(Boolean.FALSE, task.documentJudgments().groupHasInconsistencyWithoutUnlabeled(docGroup));
	}
	
	@Test
	public void testThatSmallGroupWithUnknownLabelsHasZeroDocumentsToRejudge() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"ADASDAS1\", \"ADASDAS2\", \"ADASDAS3\", \"ADASDAS4\"]}");
		
		Assert.assertEquals(0, task.documentJudgments().documentsToJudgeAgain(docGroup));
	}
	
	@Test
	public void testThatSmallGroupWithMultipleLabelsHasNoInconsistency() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"ADASDAS1\", \"ADASDAS2\", \"ADASDAS3\", \"ADASDAS4\"]}");
		
		Assert.assertEquals(Boolean.TRUE, task.documentJudgments().groupHasInconsistency(docGroup));
		Assert.assertEquals(Boolean.FALSE, task.documentJudgments().groupHasInconsistencyWithoutUnlabeled(docGroup));
	}
	
	@Test
	public void testThatSmallGroupSingleLabelsHasNoInconsistency() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"552901\"]}");
		
		Assert.assertEquals(Boolean.FALSE, task.documentJudgments().groupHasInconsistency(docGroup));
		Assert.assertEquals(Boolean.FALSE, task.documentJudgments().groupHasInconsistencyWithoutUnlabeled(docGroup));
	}
	
	@Test
	public void testThatSmallGroupMultipleLabelsHasNoInconsistency() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"552301\"]}");
		
		Assert.assertEquals(Boolean.TRUE, task.documentJudgments().groupHasInconsistency(docGroup));
		Assert.assertEquals(Boolean.TRUE, task.documentJudgments().groupHasInconsistencyWithoutUnlabeled(docGroup));
	}
	
	@Test
	public void testThatSmallGroupWithMultipleLabelsHasSomeDocumentsToRejudge() {
		SharedTask task = TrecSharedTask.CORE_2017;
		DocumentGroup docGroup = new DocumentGroup("{\"hash\": \"a\", \"ids\": [\"552901\", \"ADASDAS1\", \"ADASDAS2\", \"ADASDAS3\", \"ADASDAS4\"]}");
		
		Assert.assertEquals(5, task.documentJudgments().documentsToJudgeAgain(docGroup));
	}
}
