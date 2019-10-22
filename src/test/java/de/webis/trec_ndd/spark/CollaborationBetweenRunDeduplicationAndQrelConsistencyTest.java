package de.webis.trec_ndd.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import de.webis.trec_ndd.spark.RunResultDeduplicator.DeduplicationResult;
import de.webis.trec_ndd.trec_collections.Qrel;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;

public class CollaborationBetweenRunDeduplicationAndQrelConsistencyTest {
	
	private List<DocumentGroup> docGroups;
	
	private List<RunLine> runLinesFullOfDuplicates;
	
	private List<RunLine> runLinesWithoutDuplicates;
	
	private List<RunLine> runLinesWithOneDuplicate;
	
	private HashSet<QrelEqualWithoutScore> qrels;
	
	@Before
	public void prepare() {
		docGroups = Arrays.asList(
			new DocumentGroup("{\"hash\":\"FooBar\",\"ids\":[\"g_1_1\",\"g_1_2\",\"g_1_3\"]}")
		);
		runLinesFullOfDuplicates = runLinesFullOfDuplicates();
		runLinesWithoutDuplicates = runLinesWithoutDuplicates();
		runLinesWithOneDuplicate = runLinesWithOneDuplicate();
		qrels = exampleQrels();
	}
	
	@Test
	public void approveDeduplicationForBase() {
		DeduplicationResult expected = new DeduplicationResult(runLinesFullOfDuplicates(), Collections.emptyMap());
		DeduplicationResult actual = RunResultDeduplicator.base.deduplicateRun(runLinesFullOfDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDeduplicationForBase2() {
		DeduplicationResult expected = new DeduplicationResult(runLinesWithoutDuplicates(), Collections.emptyMap());
		DeduplicationResult actual = RunResultDeduplicator.base.deduplicateRun(runLinesWithoutDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDeduplicationForBase3() {
		DeduplicationResult expected = new DeduplicationResult(runLinesWithOneDuplicate(), Collections.emptyMap());
		DeduplicationResult actual = RunResultDeduplicator.base.deduplicateRun(runLinesWithOneDuplicate, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDeduplicationForMax() {
		List<RunLine> expectedRunLines = toRunLines(
			"1 Q0 g_1_2 1 10 FooBar",
			"2 Q0 g_1_3 1 10 FooBar"
		);
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
			.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
			.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
			.build();
		
		DeduplicationResult expected = new DeduplicationResult(expectedRunLines, topicToDocumentIdsToMakeIrrelevant);
		DeduplicationResult actual = RunResultDeduplicator.removeDuplicates.deduplicateRun(runLinesFullOfDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}

	
	@Test
	public void approveDeduplicationForMax2() {
		DeduplicationResult expected = new DeduplicationResult(runLinesWithoutDuplicates(), Collections.emptyMap());
		DeduplicationResult actual = RunResultDeduplicator.removeDuplicates.deduplicateRun(runLinesWithoutDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDeduplicationForMax3() {
		List<RunLine> expectedRunLines = toRunLines(
			"1 Q0 no_group_1_2 1 10 FooBar",
			"1 Q0 g_1_1 2 9 FooBar",
			"1 Q0 no_group_1_3 3 8 FooBar",
					
			"2 Q0 no_group_3 1 10 FooBar",
			"2 Q0 no_group_1_1 2 9 FooBar",
			"2 Q0 g_1_2 3 8 FooBar"
		);
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
			.put(2, new HashSet<>(Arrays.asList("g_1_1")))
			.build();
		
		DeduplicationResult expected = new DeduplicationResult(expectedRunLines, topicToDocumentIdsToMakeIrrelevant);
		DeduplicationResult actual = RunResultDeduplicator.removeDuplicates.deduplicateRun(runLinesWithOneDuplicate, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	
	@Test
	public void approveDeduplicationForMarkedIrrelevant() {
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
			.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
			.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
			.build();
		
		DeduplicationResult expected = new DeduplicationResult(runLinesFullOfDuplicates(), topicToDocumentIdsToMakeIrrelevant);
		DeduplicationResult actual = RunResultDeduplicator.duplicatesMarkedIrrelevant.deduplicateRun(runLinesFullOfDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void approveDeduplicationForMarkedIrrelevant2() {
		DeduplicationResult expected = new DeduplicationResult(runLinesWithoutDuplicates(), Collections.emptyMap());
		DeduplicationResult actual = RunResultDeduplicator.duplicatesMarkedIrrelevant.deduplicateRun(runLinesWithoutDuplicates, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDeduplicationForMarkedIrrelevant3() {
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
			.put(2, new HashSet<>(Arrays.asList("g_1_1")))
			.build();
		
		DeduplicationResult expected = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		DeduplicationResult actual = RunResultDeduplicator.duplicatesMarkedIrrelevant.deduplicateRun(runLinesWithOneDuplicate, docGroups);
	
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatBaseQrelConsistencyDoesNothing() {
		QrelConsistentMaker base = QrelConsistentMaker.base;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
				.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		
		Assert.assertEquals(toQrel(exampleQrels()), toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatBaseQrelConsistencyDoesNothing2() {
		QrelConsistentMaker base = QrelConsistentMaker.base;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(2, new HashSet<>(Arrays.asList("g_1_1")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		
		Assert.assertEquals(toQrel(exampleQrels()), toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatBaseQrelConsistencyDoesNothing3() {
		QrelConsistentMaker base = QrelConsistentMaker.base;
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), Collections.emptyMap());
		
		Assert.assertEquals(toQrel(exampleQrels()), toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxQrelConsistencyDoesNothing() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValue;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
				.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 2",
				"1 0 g_1_3 2",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",

				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 2",
				"2 0 g_1_2 2"
		);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxQrelConsistencyDoesNothing2() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValue;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(2, new HashSet<>(Arrays.asList("g_1_1")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 2",
				"1 0 g_1_3 2",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 2",
				"2 0 g_1_2 2"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxQrelConsistencyDoesNothing3() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValue;
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), Collections.emptyMap());
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 2",
				"1 0 g_1_3 2",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 2",
				"2 0 g_1_2 2"
			);
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxWithDuplicateDocsIrrelevantQrelConsistencyDoesNothing() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueDuplicateDocsIrrelevant;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
				.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(

				"1 0 g_1_1 0",
				"1 0 g_1_2 2",
				"1 0 g_1_3 0",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 0",
				"2 0 g_1_2 0"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxWithDuplicateDocsIrrelevantQrelConsistencyDoesNothing2() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueDuplicateDocsIrrelevant;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(2, new HashSet<>(Arrays.asList("g_1_1")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 2",
				"1 0 g_1_3 2",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 0",
				"2 0 g_1_2 2"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxWithDuplicateDocsIrrelevantQrelConsistencyDoesNothing3() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueDuplicateDocsIrrelevant;
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), Collections.emptyMap());
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 2",
				"1 0 g_1_3 2",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 2",
				"2 0 g_1_2 2"
			);
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	

	@Test
	public void checkThatMaxWithAllDuplicateDocsIrrelevantQrelConsistencyDoesNothing() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueAllDuplicateDocsIrrelevant;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(1, new HashSet<>(Arrays.asList("g_1_1", "g_1_3")))
				.put(2, new HashSet<>(Arrays.asList("g_1_1", "g_1_2")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 0",
				"1 0 g_1_2 2",
				"1 0 g_1_3 0",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 2",
				"2 0 g_1_1 0",
				"2 0 g_1_2 0"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxWithAllDuplicateDocsIrrelevantQrelConsistencyDoesNothing2() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueAllDuplicateDocsIrrelevant;
		Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant = ImmutableMap.<Integer, Set<String>>builder()
				.put(2, new HashSet<>(Arrays.asList("g_1_1")))
				.build();
			
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), topicToDocumentIdsToMakeIrrelevant);
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 0",
				"1 0 g_1_3 0",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 0",
				"2 0 g_1_1 0",
				"2 0 g_1_2 2"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	@Test
	public void checkThatMaxWithAllDuplicateDocsIrrelevantQrelConsistencyDoesNothing3() {
		QrelConsistentMaker base = QrelConsistentMaker.maxValueAllDuplicateDocsIrrelevant;
		DeduplicationResult deduplication = new DeduplicationResult(runLinesWithOneDuplicate(), Collections.emptyMap());
		HashSet<Qrel> expected = toExpectedQrels(
				"1 0 g_1_1 2",
				"1 0 g_1_2 0",
				"1 0 g_1_3 0",
				"1 0 no_group_1_2 3",
				"1 0 no_group_1_3 5",
						
				"2 0 no_group_3_1 2",
				"2 0 no_group_1_1 4",
				"2 0 g_1_3 0",
				"2 0 g_1_1 2",
				"2 0 g_1_2 0"
			);
		
		Assert.assertEquals(expected, toQrel(base.getQrels(qrels, docGroups, deduplication)));
	}
	
	private static List<RunLine> toRunLines(String...lines) {
		return Stream.<String>of(lines)
			.map(RunLine::new)
			.collect(Collectors.toList());
	}
	
	private static List<RunLine> runLinesFullOfDuplicates() {
		return toRunLines(
			"1 Q0 g_1_2 1 10 FooBar",
			"1 Q0 g_1_1 2 9 FooBar",
			"1 Q0 g_1_3 3 8 FooBar",

			"2 Q0 g_1_3 1 10 FooBar",
			"2 Q0 g_1_1 2 9 FooBar",
			"2 Q0 g_1_2 3 8 FooBar"
		);
	}
	
	private static List<RunLine> runLinesWithoutDuplicates() {
		return toRunLines(
				"1 Q0 no_group_1_2 1 10 FooBar",
				"1 Q0 g_1_1 2 9 FooBar",
				"1 Q0 no_group_1_3 3 8 FooBar",
					
				"2 Q0 no_group_3 1 10 FooBar",
				"2 Q0 no_group_1_1 2 9 FooBar",
				"2 Q0 g_1_2 3 8 FooBar"
			);
	}
	
	private static List<RunLine> runLinesWithOneDuplicate() {
		return toRunLines(
				"1 Q0 no_group_1_2 1 10 FooBar",
				"1 Q0 g_1_1 2 9 FooBar",
				"1 Q0 no_group_1_3 3 8 FooBar",
					
				"2 Q0 no_group_3 1 10 FooBar",
				"2 Q0 no_group_1_1 2 9 FooBar",
				"2 Q0 g_1_2 3 8 FooBar",
				"2 Q0 g_1_1 4 7 FooBar"
			);
	}
	
	private HashSet<QrelEqualWithoutScore> exampleQrels() {
		return toQrels(
			"1 0 g_1_1 2",
			"1 0 g_1_2 1",
			"1 0 g_1_3 0",
			"1 0 no_group_1_2 3",
			"1 0 no_group_1_3 5",
					
			"2 0 no_group_3_1 2",
			"2 0 no_group_1_1 4",
			"2 0 g_1_3 0",
			"2 0 g_1_1 1",
			"2 0 g_1_2 2"
		);
	}
	
	public static HashSet<Qrel> toExpectedQrels(String...qrels) {
		return new HashSet<>(Stream.<String>of(qrels)
				.map(Qrel::new)
				.collect(Collectors.toList()));
	}
	
	public static HashSet<Qrel> toQrel(Set<QrelEqualWithoutScore> qrels) {
		return new HashSet<>(qrels.stream()
				.map(q -> {
					Qrel ret = new Qrel();
					ret.setDocumentID(q.getDocumentID());
					ret.setScore(q.getScore());
					ret.setTopicNumber(q.getTopicNumber());
					return ret;
				})
				.collect(Collectors.toSet()));
	}
	
	public static HashSet<QrelEqualWithoutScore> toQrels(String...qrels) {
		return new HashSet<>(Stream.<String>of(qrels)
			.map(QrelEqualWithoutScore::new)
			.collect(Collectors.toList()));
	}
}
