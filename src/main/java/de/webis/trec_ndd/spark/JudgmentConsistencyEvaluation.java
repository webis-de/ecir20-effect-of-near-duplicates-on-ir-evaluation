package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;

import static de.webis.trec_ndd.spark.SparkDeduplicateAndEvaluate.readGroupsUsedInRunFiles;

import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.DocumentJudgments;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class JudgmentConsistencyEvaluation implements SparkArguments {
	
	@Getter
	private final Namespace parsedArgs;
	
	public JudgmentConsistencyEvaluation(String[] args) {
		this.parsedArgs = parseArguments(args);
	}
	
	@Override
	public void run() {
		try (JavaSparkContext sc = context()) {
			for(TrecCollections collection: collections()) {
				String retrievalEquivalentDir = SparkGroupByFingerprint.resultDir(collection, documentSelection(), documentSimilarity());
				List<DocumentGroup> retrievalEquivalent = readGroupsUsedInRunFiles(sc, retrievalEquivalentDir, collection); 
				String contentEquivalentDir = SparkGroupByFingerprint.resultDir(collection, documentSelection(), "s3-similarity-connected-component-" + s3Threshold());
				List<DocumentGroup> contentEquivalent = readGroupsUsedInRunFiles(sc, contentEquivalentDir, collection);
				List<DocumentGroup> contentEquivalentWithoutRetrievalEquivalentOnly = new ArrayList<>(contentEquivalent.stream().map(DocumentGroup::copy).collect(Collectors.toList()));
				contentEquivalent = DocumentGroup.appendRetrievalEquivalentToContentEquivalent(contentEquivalent, retrievalEquivalent);
				
				DocumentGroups documentGroups = DocumentGroups.builder()
						.contentEquivalent(contentEquivalent)
						.retrievalEquivalent(retrievalEquivalent)
						.contentEquivalentWithoutRetrievalEquivalent(contentEquivalentWithoutRetrievalEquivalentOnly)
						.build();
				
				for(SharedTask task : collection.getSharedTasks()) {
					report(documentGroups, task);
				}
			}
		}
	}
	
	public static void main(String[] args) {
		new JudgmentConsistencyEvaluation(args).run();
	}
	
	@SneakyThrows
	private static void report(DocumentGroups docGroups, SharedTask task) {
		DocumentJudgments docJudgments = task.documentJudgments();
		
		Map<String, Object> report = ImmutableMap.<String, Object>builder()
				.put("SharedTask", task.name())
				.put("GroupSize", docGroups.getContentEquivalent().size())
				.put("InconsistentGroups", docGroups.inconsistentGroups(docJudgments))
				.put("InconsistentGroups(WithoutUnlabeled)", docGroups.inconsistentGroupsWithoutUnlabeled(docJudgments))
				.put("InconsistentGroups(WithoutUnlabeledAndRetrievalEquivalent)", docGroups.inconsistentGroupsWithoutUnlabeledWithoutRetrievalEquivalent(docJudgments))
				.put("ConsistentGroups", docGroups.consistentGroups(docJudgments))
				.put("JudgmentCount", docJudgments.judgmentCount())
				.put("DocumentsToJudgeAgain", docGroups.documentsToJudgeAgain(docJudgments))
				.put("TopicDetails", docGroups.topicDetails(docJudgments))
				.build();
		
		System.out.println(new ObjectMapper().writeValueAsString(report));
	}

	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("JudgmentConsistencyEvaluation")
				.build()
				.defaultHelp(true)
				.description("Evaluate inconsistencies caused by near-duplicates in the QRELs.");
		
		addCollectionsToArgparser(parser);
		addSimilarityToArgparser(parser);
		addDocumentSelectionToArgparser(parser);
		addS3Threshold(parser);
		
		return parser.parseArgsOrFail(args);
	}
	
	@Data
	@Builder
	public static class DocumentGroups {
		private List<DocumentGroup> contentEquivalent,
									retrievalEquivalent,
									contentEquivalentWithoutRetrievalEquivalent;
		
		public int inconsistentGroups(DocumentJudgments docJudgments) {
			return contentEquivalent.parallelStream()
				.mapToInt(group -> docJudgments.groupHasInconsistency(group) ? 1 : 0)
				.sum();
		}
		
		public int inconsistentGroupsWithoutUnlabeled(DocumentJudgments docJudgments) {
			return contentEquivalent.parallelStream()
				.mapToInt(group -> docJudgments.groupHasInconsistencyWithoutUnlabeled(group) ? 1 : 0)
				.sum();
		}
		
		public Object inconsistentGroupsWithoutUnlabeledWithoutRetrievalEquivalent(DocumentJudgments docJudgments) {
			return contentEquivalentWithoutRetrievalEquivalent.parallelStream()
				.mapToInt(group -> docJudgments.groupHasInconsistencyWithoutUnlabeled(group) ? 1 : 0)
				.sum();
		}

		public int consistentGroups(DocumentJudgments docJudgments) {
			return contentEquivalent.parallelStream()
				.mapToInt(group -> docJudgments.groupHasInconsistency(group) ? 0 : 1)
				.sum();
		}
		
		public int documentsToJudgeAgain(DocumentJudgments docJudgments) {
			return contentEquivalent.parallelStream()
				.mapToInt(docJudgments::documentsToJudgeAgain)
				.sum();
		}

		public Map<String, Map<String, Integer>> topicDetails(DocumentJudgments docJudgments) {
			return docJudgments.topics().stream()
				.collect(Collectors.toMap(i -> i, i -> topicDetails(i, docJudgments)));
		}
		
		private Map<String, Integer> topicDetails(String topic, DocumentJudgments docJudgments) {
			Set<String> relevantJudgments = new HashSet<>(docJudgments.getRelevantDocuments(topic));
			Set<String> irrelevantJudgments = new HashSet<>(docJudgments.getIrrelevantDocuments(topic));
			Set<String> retrievalEquivalent = getRetrievalEquivalent().stream()
				.flatMap(i -> i.ids.stream())
				.collect(Collectors.toSet());
			Set<String> contentEquivalent = getContentEquivalent().stream()
					.flatMap(i -> i.ids.stream())
					.collect(Collectors.toSet());
			
			failOnDocumentsThatAreRetrievalEquivalentButNotContentEquivalent(retrievalEquivalent, contentEquivalent, topic);
			
			return ImmutableMap.<String, Integer>builder()
				.put("relevantJudgments", relevantJudgments.size())
				.put("retrievalEquivalentGroupsInRelevantJudgments", groupsInIntersection(relevantJudgments, getRetrievalEquivalent()))
				.put("contentEquivalentGroupsInRelevantJudgments", groupsInIntersection(relevantJudgments, getContentEquivalent()))
				.put("retrievalEquivalentDocsInRelevantJudgments", intersection(retrievalEquivalent, relevantJudgments))
				.put("contentEquivalentDocsInRelevantJudgments", intersection(contentEquivalent, relevantJudgments))
				.put("irrelevantJudgments", irrelevantJudgments.size())
				.put("retrievalEquivalentDocsInIrrelevantJudgments", intersection(retrievalEquivalent, irrelevantJudgments))
				.put("contentEquivalentDocsInIrrelevantJudgments", intersection(contentEquivalent, irrelevantJudgments))
				.put("retrievalEquivalentGroupsInIrrelevantJudgments", groupsInIntersection(irrelevantJudgments, getRetrievalEquivalent()))
				.put("contentEquivalentGroupsInIrrelevantJudgments", groupsInIntersection(irrelevantJudgments, getContentEquivalent()))
				.build();
		}
		
		private static void failOnDocumentsThatAreRetrievalEquivalentButNotContentEquivalent(Set<String> retrievalEquivalent, Set<String> contentEquivalent, String topic) {
			List<String> notContentEquivalent = new ArrayList<>();
			
			for(String retrievalEq : retrievalEquivalent) {
				if(!contentEquivalent.contains(retrievalEq)) {
					notContentEquivalent.add(retrievalEq);
				}
			}
			
			if(notContentEquivalent.size() > 0) {
				throw new RuntimeException("Unexpected Behaviour for topic '" + topic + "': Documents are retrieval-equivalent but not content-equivalent: "+ notContentEquivalent);
			}
		}
		
		private int groupsInIntersection(Set<String> a, List<DocumentGroup> b) {
			int ret = 0;
			
			for(DocumentGroup docGroup: b) {
				if(docGroup.ids.stream().anyMatch(i -> a.contains(i))) {
					ret += 1;
				}
			}
			
			return ret;
		}
		
		private <T> int intersection(Set<T> a, Set<T> b) {
			Set<T> ret = new HashSet<T>(a);
			ret.retainAll(b);
			
			return ret.size();
		}
	}

	@Override
	public String jobName() {
		return "trec-qrel-consistency";
	}
}
