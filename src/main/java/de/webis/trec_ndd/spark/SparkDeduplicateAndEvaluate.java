package de.webis.trec_ndd.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;

import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.spark.RunResultDeduplicator.DeduplicationResult;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_eval.EvaluationMeasure;
import de.webis.trec_ndd.trec_eval.TrecEvaluation;
import de.webis.trec_ndd.trec_eval.TrecEvaluator;
import de.webis.trec_ndd.util.AverageRunMeasure;
import de.webis.trec_ndd.util.MutualExecution;
import de.webis.trec_ndd.util.ParticipationModel;
import de.webis.trec_ndd.util.RankCorrelation;
import de.webis.trec_ndd.util.ScoringChanges;
import de.webis.trec_ndd.util.SystemRankingPreprocessing;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static de.webis.trec_ndd.spark.SparkGroupByFingerprint.resultDir;

public class SparkDeduplicateAndEvaluate implements SparkArguments {

	@Getter
	private final Namespace parsedArgs;
	
	private SparkDeduplicateAndEvaluate(String[] args) {
		this.parsedArgs = parseArguments(args);
	}
	
	@Override
	public void run() {
		List<DocumentGroup> contentEquivalentGroups = contentEquivalentDocumentGroups();
		for(SharedTask sharedTask : collection().getSharedTasks()) {
			Set<QrelEqualWithoutScore> qrels = sharedTask.getQrelResourcesWithoutScore();
			
			EvaluationStrategy.allEvaluationStrategies(sharedTask, qrels, contentEquivalentGroups).forEach(evaluationStrategy ->{
				if(MutualExecution.shouldExecuteTogether(evaluationStrategy.getQrelConsistentMaker(), evaluationStrategy.getRunDeduplicator())) {
					reportOrFail(evaluationStrategy);	
				}
			});
		}
	}
	
	public static void main(String[] args) {
		new SparkDeduplicateAndEvaluate(args).run();
	} 
	
	private void reportOrFail(EvaluationStrategy strategy) {
		try {
			report(strategy);
		} catch (Exception e) {
			
			throw new RuntimeException("Failed to report strategy for shared task: " 
				+ strategy.getSharedTask().name() + " and dedup: " 
				+ strategy.getRunDeduplicator().name() + " and qrel-consistency "
				+ strategy.getQrelConsistentMaker().name() + " and preprocessing "
				+ strategy.getSystemRankingPreprocessing().name() + " and measure "
				+ strategy.getEvaluationMeasure().name(), e);
		}
	}
	
	private void report(EvaluationStrategy strategy) throws Exception {
		List<Pair<String,Double>> originalRanking = strategy.baseline();
		List<Pair<String,Double>> manipulatedRanking = strategy.applyStrategy();
		originalRanking = strategy.getSystemRankingPreprocessing().preprocessOriginalRanking(originalRanking);
		manipulatedRanking = strategy.getSystemRankingPreprocessing().preprocessManipulatedRanking(
				originalRanking.stream().map(Pair::getLeft).collect(Collectors.toSet()),
				manipulatedRanking);

		double kendallTau = RankCorrelation.sortListsAndCalculateKendallTau(manipulatedRanking, originalRanking);
		List<Double> scoreChangesPerRank = new ScoringChanges(originalRanking).calculateScoringChangesTo(manipulatedRanking);;
		ParticipationModel participantModel = new ParticipationModel(originalRanking);
		
		Map<String, Object> report = ImmutableMap.<String, Object>builder()
				.put("SharedTask", strategy.getSharedTask().name())
				.put("QrelConsistency", strategy.getQrelConsistentMaker().name())
				.put("EvaluationMeasure", strategy.getEvaluationMeasure().name())
				.put("Deduplication", strategy.getRunDeduplicator().name())
				.put("KendallTau", kendallTau)
				.put("KendallTau@5", RankCorrelation.sortedKendallTauAtK(manipulatedRanking, originalRanking, 5))
				.put("KendallTau@10", RankCorrelation.sortedKendallTauAtK(manipulatedRanking, originalRanking, 10))
				.put("ScoreChangesPerRank", scoreChangesPerRank)
				.put("OriginalRanking", ScoringChanges.rankingAsMap(originalRanking))
				.put("ManipulatedRanking", ScoringChanges.rankingAsMap(manipulatedRanking))
				.put("CheatingParticipantVector", participantModel.calculateCheatingVector(manipulatedRanking))
				.put("IdealParticipantVector", participantModel.calculateIdealVector(manipulatedRanking))
				.put("AverageRankEval", new AverageRunMeasure(originalRanking).reportAvg(manipulatedRanking))
				.put("SystemRankingPreprocessing", strategy.getSystemRankingPreprocessing())
				.build();
		 
		System.out.println(new ObjectMapper().writeValueAsString(report));
	}
	
	@SneakyThrows
	public static String fromString(String value) {
		HashMap<String, Object> bla = new ObjectMapper().readValue(value, HashMap.class);
		
		List<Pair<String,Double>> originalRanking = ScoringChanges.mapToRanking((Map) bla.get("OriginalRanking"));
		List<Pair<String,Double>> manipulatedRanking = ScoringChanges.mapToRanking((Map) bla.get("ManipulatedRanking"));
		bla.put("KendallTau", RankCorrelation.sortListsAndCalculateKendallTau(manipulatedRanking, originalRanking));
		bla.put("KendallTau@5",  RankCorrelation.sortedKendallTauAtK(manipulatedRanking, originalRanking, 5));
		bla.put("KendallTau@10",  RankCorrelation.sortedKendallTauAtK(manipulatedRanking, originalRanking, 10));
		
		return new ObjectMapper().writeValueAsString(ImmutableMap.<String, Object>builder()
				.put("SharedTask", bla.get("SharedTask"))
				.put("QrelConsistency", bla.get("QrelConsistency"))
				.put("EvaluationMeasure", bla.get("EvaluationMeasure"))
				.put("Deduplication", bla.get("Deduplication"))
				.put("KendallTau", bla.get("KendallTau"))
				.put("KendallTau@5", bla.get("KendallTau@5"))
				.put("KendallTau@10", bla.get("KendallTau@10"))
				.put("ScoreChangesPerRank", bla.get("ScoreChangesPerRank"))
				.put("OriginalRanking", bla.get("OriginalRanking"))
				.put("ManipulatedRanking", bla.get("ManipulatedRanking"))
				.put("CheatingParticipantVector", bla.get("CheatingParticipantVector"))
				.put("IdealParticipantVector", bla.get("IdealParticipantVector"))
				.put("AverageRankEval", bla.get("AverageRankEval"))
				.put("SystemRankingPreprocessing", bla.get("SystemRankingPreprocessing"))
				.build());
	}
	
	@Data
	private static class EvaluationStrategy {
		private final QrelConsistentMaker qrelConsistentMaker;
		private final RunResultDeduplicator runDeduplicator;
		private final EvaluationMeasure evaluationMeasure;
		private final SharedTask sharedTask;
		private final Set<QrelEqualWithoutScore> originalQrels;
		private final List<DocumentGroup> groupsUsed;
		private final SystemRankingPreprocessing systemRankingPreprocessing;
		
		private static final TrecEvaluator trec_eval = new TrecEvaluator();
		
		@SuppressWarnings("unchecked")
		public List<Pair<String,Double>> applyStrategy() {
				return UnmodifiableList.decorate( sharedTask.rankingResults()
						.map(runPair-> Pair.of(runPair.getLeft(), evaluateRun(runPair)))
						.map(p -> Pair.of(p.getLeft(), evaluationMeasure.evaluate(p.getRight(), sharedTask)))
						.sorted((a,b)->a.getRight().compareTo(b.getRight()))
						.collect(Collectors.toList())
				);
		}

		private TrecEvaluation evaluateRun(Pair<String, List<RunLine>> run) {
			DeduplicationResult deduplication = runDeduplicator.deduplicateRun(run.getRight(), groupsUsed);
			HashSet<QrelEqualWithoutScore> processedQrels = qrelConsistentMaker.getQrels(getOriginalQrels(), groupsUsed, deduplication); 
			
			return trec_eval.evaluate(run.getLeft(), processedQrels, deduplication.getDeduplicatedRun());
		}
		
		public List<Pair<String,Double>> baseline() {
			EvaluationStrategy baselineStrategy = new EvaluationStrategy(QrelConsistentMaker.base, RunResultDeduplicator.base, getEvaluationMeasure(), getSharedTask(), getOriginalQrels(), getGroupsUsed(), getSystemRankingPreprocessing());
			
			return baselineStrategy.applyStrategy();
		}
		
		public static List<EvaluationStrategy> allEvaluationStrategies(SharedTask sharedTask, Set<QrelEqualWithoutScore> qrelSetFromFile, List<DocumentGroup> groupsUsed){
			 List<EvaluationStrategy> ret = new ArrayList<>();
			 QrelConsistentMaker.all.forEach(qrelConsistentMaker->{
				RunResultDeduplicator.all(sharedTask).forEach(runDeduplicator->{
					sharedTask.getInofficialEvaluationMeasures().forEach(evaluationMeasure->{
						SystemRankingPreprocessing.ALL.forEach(systemRankingPreprocessing -> {
							ret.add(new EvaluationStrategy(qrelConsistentMaker, runDeduplicator, evaluationMeasure, sharedTask, qrelSetFromFile, groupsUsed, systemRankingPreprocessing));
						});
					});
				});
			});
			return ret;
		}
	}

	private List<DocumentGroup> contentEquivalentDocumentGroups() {
		try (JavaSparkContext sc = context()) {
			List<DocumentGroup> retrievalEquivalent = readGroupsUsedInRunFiles(
				sc,
				resultDir(collection(), documentSelection(), documentSimilarity()),
				collection()
			); 
			List<DocumentGroup> contentEquivalent = readGroupsUsedInRunFiles(
				sc,
				resultDir(collection(), documentSelection(), "s3-similarity-connected-component-" + s3Threshold()),
				collection()
			);
			
			return DocumentGroup.appendRetrievalEquivalentToContentEquivalent(
				contentEquivalent,
				retrievalEquivalent
			);
		}
	}
	
	static ArrayList<DocumentGroup> readGroupsUsedInRunFiles(JavaSparkContext sc, String groupPath, CollectionConfiguration collection) {
		HashSet <String> usedDocIDs = collection.documentIdsInRunFiles();
			
		ArrayList<DocumentGroup> usedDocGroups = sc.textFile(groupPath)
			.map(line -> new DocumentGroup(line))
			.filter(group -> group.isUsed(usedDocIDs))
			.collect()
			.stream()
			.collect(Collectors.toCollection(ArrayList::new));
			
		return usedDocGroups;
	}
	
	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("SparkHashDataset")
			.build()
			.defaultHelp(true)
			.description("Build hash-document representations for all documents in a dataset.");

		addCollectionToArgparser(parser);
		addDocumentSelectionToArgparser(parser);
		addSimilarityToArgparser(parser);
		addS3Threshold(parser);

		return parser.parseArgsOrFail(args);
	}
	
	@Override
	public String jobName() {
		return "ndd-eval";
	}
}
