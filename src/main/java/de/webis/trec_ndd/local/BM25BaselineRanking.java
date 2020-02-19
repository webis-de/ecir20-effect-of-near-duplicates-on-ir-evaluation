package de.webis.trec_ndd.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.SneakyThrows;

public class BM25BaselineRanking {
	private static final String BASE_DIR = "/home/aqqes/workspace/wstud-thesis-reimer/";
			
	@SneakyThrows
	public static void main(String[] args) {
		List<String> trainTestSplits = Files.readAllLines(Paths.get(BASE_DIR + "source/ltr/src/main/resources/clueweb09-train-test-splits.jsonl"));
		File featureVectors = new File(BASE_DIR + "source/ltr-files/src/main/resources/clueweb09/feature-vectors.json");
		
		for(String trainTestSplit: trainTestSplits) {
			String runFile = featureVectorsToRunFile(featureVectors, trainTestSplit);
			Path resultPath = rerankedFile(trainTestSplit).toPath();
			
			System.out.println("Create BM25 ranking for " + resultPath);
			resultPath.getParent().toFile().mkdirs();
			resultPath.toFile().createNewFile();
			Files.write(resultPath, runFile.getBytes(StandardCharsets.UTF_8));
			
			Path trainingResult = resultPath.getParent().resolve("reranked-training");
			trainTestSplit = StringUtils.replace(trainTestSplit, "test", "BLAAA-HACK");
			trainTestSplit = StringUtils.replace(trainTestSplit, "train", "test");

			runFile = featureVectorsToRunFile(featureVectors, trainTestSplit);
			System.out.println("Create BM25 ranking for " + trainingResult);
			Files.write(trainingResult, runFile.getBytes(StandardCharsets.UTF_8));
			
			Path experimentDetailsPath = resultPath.getParent().resolve("experiment-result-details.json");
			Files.write(experimentDetailsPath, "{\"trainingSetSize\": 0, \"firstWikipediaOccurrences\": []}".getBytes(StandardCharsets.UTF_8));
		}
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static File rerankedFile(String json) {
		Map<String, Object> tmp = new ObjectMapper().readValue(json, Map.class);
		String strategyName = (String) tmp.get("name");
		
		File ret = App.BASE_DIR.toPath()
				.resolve(strategyName)
				.resolve("deduplicate-relevant-keep-irrelevant")
				.resolve("BM25")
				.resolve("Map")
				.resolve("no-explicit-oversampling")
				.resolve("execution-1").toFile();
		
		ret.mkdirs();
		
		return ret.toPath().resolve("reranked").toFile();
	}
	
	@SneakyThrows
	public static String featureVectorsToRunFile(File file, String trainTestSplitJson) {
		return featureVectorsToRunFile(new FileInputStream(file), trainTestSplitJson);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	public static String featureVectorsToRunFile(InputStream is, String trainTestSplitJson) {
		Map<String, Object> topicToDocToFeatures = new ObjectMapper().readValue(is, Map.class);
		String ret = "";

		for(String topic : testTopics(trainTestSplitJson)) {
			List<Pair<String, Double>> sortedDocs = sortedDocumentsForTopic(topic, topicToDocToFeatures);
			for(int pos=0; pos < sortedDocs.size(); pos++) {
				ret += topic + " Q0 " + sortedDocs.get(pos).getKey() + " " + (pos+1) + " " + sortedDocs.get(pos).getValue() + " bm25\n";
			}
		}
		
		return ret;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@SneakyThrows
	private static List<String> testTopics(String json) {
		Map<String, Object> ret = new ObjectMapper().readValue(json, Map.class);
		
		return (List) ret.get("test");
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Pair<String, Double>> sortedDocumentsForTopic(String topic, Map<String, Object> topicToDocToFeatures) {
		List<Pair<String, Double>> ret = new ArrayList<>();
		Map<String, Object> docToFeatures = (Map) topicToDocToFeatures.get(topic);
		
		for(String docId : docToFeatures.keySet()) {
			Map<String, Object> features = (Map) docToFeatures.get(docId);
			Double bm25 = (Double) features.get("body-bm25-similarity");
			
			ret.add(Pair.of(docId, bm25));
		}
		
		Collections.sort(ret, (a, b) -> b.getRight().compareTo(a.getRight()));
		
		return ret;
	}
}
