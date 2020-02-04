package de.webis.trec_ndd.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.approvaltests.Approvals;
import org.junit.Before;
import org.junit.Test;

import de.webis.trec_ndd.spark.JudgmentConsistencyEvaluation.DocumentGroups;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.DocumentJudgments;
import de.webis.trec_ndd.trec_eval.EvaluationMeasure;

public class JudgmentConsistencyEvaluationTest {
	
	private List<DocumentGroup> contentEquivalent;
	
	private List<DocumentGroup> retrievalEquivalent;

	@Before
	public void prepare() {
		contentEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"g_1_1\",\"g_1_2\",\"g_1_3\"]}"),
			new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"g_2_1\",\"g_2_2\"]}")
		);
		retrievalEquivalent = Arrays.asList(
			new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"g_1_1\",\"g_1_2\"]}")
		);
	}
	
	@Test
	public void checkTopicDetailsOnSmallExample() {
		DocumentGroups docGroups = DocumentGroups.builder()
			.contentEquivalent(contentEquivalent)
			.retrievalEquivalent(retrievalEquivalent)
			.build();
		
		DocumentJudgments documentJudgments = smallExample().documentJudgments();
		
		Approvals.verifyAsJson(docGroups.topicDetails(documentJudgments));
	}
	
	@Test
	public void checkTopicDetailsOnSmallExample2() {
		DocumentGroups docGroups = DocumentGroups.builder()
			.contentEquivalent(contentEquivalent)
			.retrievalEquivalent(retrievalEquivalent)
			.build();
		
		DocumentJudgments documentJudgments = smallExample2().documentJudgments();
		
		Approvals.verifyAsJson(docGroups.topicDetails(documentJudgments));
	}
	
	private SharedTask smallExample() {
		return sharedTask(
			"1 0 g_1_1 2",
			"1 0 g_1_2 1",
			"1 0 g_1_3 0",
			"1 0 no_group_1_2 3",
			"1 0 no_group_1_3 5",
			
			"2 0 no_group_3 2",
			"2 0 no_group_1_1 4",
			"2 0 g_1_3 0",
			"2 0 g_1_1 1",
			"2 0 g_1_2 2"
		);
	}
	
	private SharedTask smallExample2() {
		return sharedTask(
			"3 0 d1 2",
			"3 0 g_2_1 0",
			"3 0 g_1_2 0",
			"3 0 no_group_1_2 3",
			"3 0 no_group_1_3 5",
			
			"4 0 d5 2",
			"4 0 d6 4",
			"4 0 d7 0",
			"4 0 d8 1",
			"4 0 d9 2"
		);
	}
	
	private SharedTask sharedTask(String...qrels) {
		return new SharedTask() {
			@Override
			public List<String> runFiles() {
				return null;
			}
			
			@Override
			public String name() {
				return null;
			}
			
			@Override
			public String getQrelResource() {
				return null;
			}
			
			@Override
			public List<EvaluationMeasure> getOfficialEvaluationMeasures() {
				return null;
			}
			
			@Override
			public List<EvaluationMeasure> getInofficialEvaluationMeasures() {
				return null;
			}
			
			public Set<QrelEqualWithoutScore> getQrelResourcesWithoutScore() {
				return Collections.unmodifiableSet(new HashSet<>(CollaborationBetweenRunDeduplicationAndQrelConsistencyTest.toQrels(qrels)));
			}

			@Override
			public Map<String, Map<String, String>> topicNumberToTopic() {
				return null;
			}
		};
	}
}
