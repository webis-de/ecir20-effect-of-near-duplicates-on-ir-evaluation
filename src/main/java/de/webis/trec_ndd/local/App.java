package de.webis.trec_ndd.local;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;

import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.spark.QrelConsistentMaker;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.spark.RunResultDeduplicator;
import de.webis.trec_ndd.spark.RunResultDeduplicator.DeduplicationResult;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import de.webis.trec_ndd.trec_eval.TrecEvaluation;
import de.webis.trec_ndd.trec_eval.TrecEvaluator;
import lombok.Data;
import lombok.SneakyThrows;
import uk.ac.gla.terrier.jtreceval.EvalReport;

public class App {
	private static final File BASE_DIR = new File("experiment-results-wip");
	
	private static final List<QrelConsistentMaker> QREL_CONSISTENCIES = Arrays.asList(
		QrelConsistentMaker.base, QrelConsistentMaker.maxValueDuplicateDocsIrrelevant);
	
	@Data
	private static class Experiment {
		private final String trainTestSplitStrategy, redundancy, algorithm, measure, explicitOversampling;
		private final List<File> experimentDirs;
	}

	@SneakyThrows
	public static void main(String[] args) {
		List<Map<String, Object>> ret = new CopyOnWriteArrayList<>();
		
		experimentSettings().parallelStream()
			.forEach(i -> ret.addAll(evaluateExperiment(i)));
		
		new ObjectMapper().writeValue(new File("evaluation-of-experiments.json"), ret);
	}
	
	private static List<Map<String, Object>> evaluateExperiment(Experiment experiment) {
			List<List<RunLine>> runs = experiment.experimentDirs.stream()
					.map(i -> RunLine.parseRunlines(i.toPath().resolve("reranked")))
					.collect(Collectors.toList());
			List<List<RunLine>> trainingRuns = experiment.experimentDirs.stream()
					.map(i -> RunLine.parseRunlines(i.toPath().resolve("reranked-training")))
					.collect(Collectors.toList());
			List<Map<String, Object>> ret = new LinkedList<>();
			
			for(RunResultDeduplicator deduplicator: RunResultDeduplicator.all(null)) {
				for(QrelConsistentMaker qrelConsistency : QREL_CONSISTENCIES) {
					
					System.out.println("---->" + new Date());
					Map<String, Object> evaluation = eval(runs, deduplicator, qrelConsistency, experiment, trainingRuns);
					evaluation.put("firstWikipediaOccurences", allWikipediaRanks(experiment));
					evaluation.put("examplesInTrainingSet", trainingSetSize(experiment));
					ret.add(evaluation);
				}
			}
			
			return ret;
	}
	
	@SneakyThrows
	private static int trainingSetSize(Experiment experiment) {
		List<Integer> ret = new LinkedList<>();
		ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});
		
		for(File f : experiment.experimentDirs) {
			Map<String, Object> m = reader.readValue(f.toPath().resolve("experiment-result-details.json").toFile());
			ret.add((int)m.get("trainingSetSize"));
		}
		
		return (int) ret.stream().mapToInt(i -> i).average().getAsDouble();
	}
	
	@SneakyThrows
	private static List<Integer> allWikipediaRanks(Experiment experiment) {
//		List<Integer> ret = new LinkedList<>();
//		ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});
//		
//		for(File f : experiment.experimentDirs) {
//			Map<String, Object> m = reader.readValue(f.toPath().resolve("experiment-result-details.json").toFile());
//			ret.addAll((List) m.get("firstWikipediaOccurrences"));
//		}
		
		return new ArrayList<>();
	}
	
	private static Map<String, Object> eval(List<List<RunLine>> runs, RunResultDeduplicator deduplicator, QrelConsistentMaker qrelConsistency, Experiment experiment, List<List<RunLine>> trainingRuns) {
		Map<String, Object> ret = new HashMap<>();
		List<DocumentGroup> docGroups = DocumentGroup.readFromJonLines(Paths.get("../wstud-thesis-reimer/source/groups/src/main/resources/trec-fingerprint-groups-clueweb09-judged.jsonl"));
		List<Double> ndcg = new LinkedList<>();
		List<Double> ndcgAtAllTopics = new LinkedList<>();
		
		List<Double> map = new LinkedList<>();
		List<Double> mapAtAllTopics = new LinkedList<>();
		
		List<Double> ndcgAt20AllTopics = new LinkedList<>();
		List<Double> ndcgAt20 = new LinkedList<>();
		
		for(List<RunLine> run: runs) {
			Set<QrelEqualWithoutScore> judgements = collectionJudgments(run, TrecCollections.CLUEWEB09);
			TrecEvaluator trec_eval = new TrecEvaluator();
			DeduplicationResult deduplication = deduplicator.deduplicateRun(run, docGroups);
			HashSet<QrelEqualWithoutScore> processedQrels = qrelConsistency.getQrels(judgements, docGroups, deduplication); 
			TrecEvaluation trecEvaluation = trec_eval.evaluate("can-be-ignored", processedQrels, deduplication.getDeduplicatedRun());

			EvalReport ndcgReport = trecEvaluation.evalReportForMeasure("ndcg");
			ndcg.add(ndcgReport.amean);
			ndcgAtAllTopics.addAll(ndcgReport.scorePerTopic);
			
			EvalReport mapReport = trecEvaluation.evalReportForMeasure("map");
			map.add(mapReport.amean);
			mapAtAllTopics.addAll(mapReport.scorePerTopic);
			
			EvalReport ndcg20Report = trecEvaluation.ndcg20Report();
			ndcgAt20.add(ndcg20Report.amean);
			ndcgAt20AllTopics.addAll(ndcg20Report.scorePerTopic);
		}
		
		List<Double> ndcgTrain = new LinkedList<>();
		List<Double> ndcgTrainAtAllTopics = new LinkedList<>();
		
		List<Double> ndcgTrainAt20AllTopics = new LinkedList<>();
		List<Double> ndcgTrainAt20 = new LinkedList<>();
		
		for(List<RunLine> trainRun: trainingRuns) {
			Set<QrelEqualWithoutScore> judgements = collectionJudgments(trainRun, TrecCollections.CLUEWEB09);
			TrecEvaluator trec_eval = new TrecEvaluator();
			DeduplicationResult deduplication = deduplicator.deduplicateRun(trainRun, docGroups);
			HashSet<QrelEqualWithoutScore> processedQrels = qrelConsistency.getQrels(judgements, docGroups, deduplication); 
			TrecEvaluation trecEvaluation = trec_eval.evaluate("can-be-ignored", processedQrels, deduplication.getDeduplicatedRun());

			EvalReport ndcgReport = trecEvaluation.evalReportForMeasure("ndcg");
			ndcgTrain.add(ndcgReport.amean);
			ndcgTrainAtAllTopics.addAll(ndcgReport.scorePerTopic);
			
			EvalReport ndcg20Report = trecEvaluation.ndcg20Report();
			ndcgTrainAt20.add(ndcg20Report.amean);
			ndcgTrainAt20AllTopics.addAll(ndcg20Report.scorePerTopic);
		}
		
		ret.put("trainTestSplitStrategy", experiment.getTrainTestSplitStrategy());
		ret.put("redundancy", experiment.getRedundancy());
		ret.put("algorithm", experiment.getAlgorithm());
		ret.put("deduplication", deduplicator.name());
		ret.put("qrelConsistency", qrelConsistency.name());
		ret.put("measure", experiment.measure);
		ret.put("explicitOversampling", experiment.explicitOversampling);
		
		ret.put("ndcg", ndcg.stream().mapToDouble(i -> i).average().getAsDouble());
		ret.put("ndcg_all_topics", ndcgAtAllTopics);
		
		ret.put("map", map.stream().mapToDouble(i -> i).average().getAsDouble());
		ret.put("map_all_topics", mapAtAllTopics);
		
		ret.put("ndcg_20", ndcgAt20.stream().mapToDouble(i -> i).average().getAsDouble());
		ret.put("ndcg_20_all_topics", ndcgAt20AllTopics);
		
		ret.put("ndcg_train", ndcgTrain.stream().mapToDouble(i -> i).average().getAsDouble());
		ret.put("ndcg_train_all_topics", ndcgTrainAtAllTopics);
		
		ret.put("ndcg_train_20", ndcgTrainAt20.stream().mapToDouble(i -> i).average().getAsDouble());
		ret.put("ndcg_train_20_all_topics", ndcgTrainAt20AllTopics);
		
		return ret;
	}

	private static Set<QrelEqualWithoutScore> collectionJudgments(List<RunLine> run, TrecCollections collection) {
		Set<QrelEqualWithoutScore> ret = new HashSet<>();
		Set<Integer> topicsInUse = run.stream()
				.map(RunLine::getTopic)
				.collect(Collectors.toSet());
		
		for(SharedTask task : collection.getSharedTasks()) {
			ret.addAll(task.getQrelResourcesWithoutScore());
		}
		
		ret = ret.stream()
				.filter(qrel -> topicsInUse.contains(qrel.getTopicNumber()))
				.collect(Collectors.toSet());
		
		return Collections.unmodifiableSet(ret);
	}
	
	private static List<Experiment> experimentSettings() {
		List<Experiment> ret = new LinkedList<>();
		
		for(String trainTestSplitStrategy : BASE_DIR.list()) {
			File tts = BASE_DIR.toPath().resolve(trainTestSplitStrategy).toFile();
			
			for(String redundancy : tts.list()) {
				File red = tts.toPath().resolve(redundancy).toFile();
				
				for(String algorithm : red.list()) {
					File algorithmDir = red.toPath().resolve(algorithm).toFile();
					
					for(String measure : algorithmDir.list()) {
						File measureDir = algorithmDir.toPath().resolve(measure).toFile();
						
						for(String explicitOversampling : measureDir.list()) {
							File explicitOversamplingDir = measureDir.toPath().resolve(explicitOversampling).toFile();
							
							List<File> experimentDirs = Arrays.asList(explicitOversamplingDir.listFiles());
					
							ret.add(new Experiment(trainTestSplitStrategy, redundancy, algorithm, measure, explicitOversampling, experimentDirs));
						}
					}
				}
			}
		}
		
		return ret;
	}
}
