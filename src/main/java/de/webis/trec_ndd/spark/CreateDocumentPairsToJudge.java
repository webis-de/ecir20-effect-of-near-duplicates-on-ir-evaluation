package de.webis.trec_ndd.spark;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.spark_project.guava.io.Files;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.Getter;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDocumentPairsToJudge implements SparkArguments {
	
	@Getter
	private final Namespace parsedArgs;
	
	@Getter
	private final DocumentSelectionStrategy documentSelection = DocumentSelectionStrategy.JUDGED;
	
	private CreateDocumentPairsToJudge(String[] args) {
		parsedArgs = parseArguments(args);
	}
	
	public static void main(String[] args) {
		new CreateDocumentPairsToJudge(args).run();
	}
	
	@Override
	public void run() {
		try(JavaSparkContext context = context()) {
			for(CollectionConfiguration collection : collections()) {
				double bucketStart = startingBucket();
				JavaRDD<S3Score> s3Scores = s3ScoresAbove(bucketStart, context, collection);
				
				while(bucketStart <= 1.0) {
					double bucketEnd = bucketStart + 0.1;
					
					sample(bucketStart, bucketEnd, collection, s3Scores, context);
					
					bucketStart = bucketEnd;
				}
			}	
		}
	}

	private void sample(double start, double end, CollectionConfiguration collection, JavaRDD<S3Score> s3Scores, JavaSparkContext context) {
		System.out.println("Sample between " + start + " -- " + end);
		List<S3Score> samples = sampleBetween(start, end, s3Scores);
		Map<String, CollectionDocument> docs = getDocsInSample(samples, collection, context);
		
		for(S3Score sample: samples) {
			Path resultPath = resultPath(collection, sample);
			
			writeDocToResult(docs.get(sample.getIdPair().getLeft()), resultPath);
			writeDocToResult(docs.get(sample.getIdPair().getRight()), resultPath);
			writeScoreToResult(sample, resultPath);
		}
	}
	
	@SneakyThrows
	private void writeScoreToResult(S3Score score, Path path) {
		String value = new ObjectMapper().writeValueAsString(score);
		Files.write(value, path.resolve("s3score").toFile(), StandardCharsets.UTF_8);
	}
	
	@SneakyThrows
	private void writeDocToResult(CollectionDocument doc, Path path) {
		Files.write(doc.getFullyCanonicalizedContent(), path.resolve(doc.getId()).toFile(), StandardCharsets.UTF_8);
		Files.write(doc.getContent(), path.resolve("full-text-" + doc.getId()).toFile(), StandardCharsets.UTF_8);
	}
	
	private Path resultPath(CollectionConfiguration c, S3Score score) {
		File resultDir = new File("results/" + collectionName(c) +"/" +
				score.getIdPair().getLeft() + "_vs_" +
				score.getIdPair().getRight());
		resultDir.mkdirs();
		
		return resultDir.toPath();
	}
	
	private Map<String, CollectionDocument> getDocsInSample(List<S3Score> sample, CollectionConfiguration collection, JavaSparkContext context) {
		Set<String> idsToKeep = new HashSet<>(); 
		sample.forEach(i -> {
			idsToKeep.add(i.getIdPair().getLeft());
			idsToKeep.add(i.getIdPair().getRight());
		});
		
		
		return context.textFile(CopyDocumentsFromRunFilesToHdfs.jobName(collectionName(collection), documentSelection))
			.map(CollectionDocument::fromString)
			.filter(i -> idsToKeep.contains(i.getId()))
			.collect().stream()
			.collect(Collectors.toMap(i -> i.getId(), i -> i));
	}

	private List<S3Score> sampleBetween(double start, double end, JavaRDD<S3Score> s3Scores) {
		Set<String> idsInTerabyte2004 = SharedTask.TrecSharedTask.TERABYTE_2004.documentIdsInRunFiles();
		
		List<S3Score> scores = s3Scores
			.filter(i -> start <= i.getS3Score() && end >= i.getS3Score())
			.filter(i -> idsInTerabyte2004.contains(i.getIdPair().getLeft()) && idsInTerabyte2004.contains(i.getIdPair().getRight()))
			.collect();
		scores = new LinkedList<>(scores);
			
		Collections.shuffle(scores);

		return scores.stream()
			.limit(pairsPerBucket())
			.collect(Collectors.toList());
	}
	
	private JavaRDD<S3Score> s3ScoresAbove(double minThreshold, JavaSparkContext context, CollectionConfiguration collection) {
//		String resultDir = SparkGroupByFingerprint.resultDir(collection, documentSelection, "intermediate-unfiltered-s3-similarity-connected-component-0.58");
		String resultDir = "intermediate-unfiltered-trec-fingerprint-groups-gov2-judged/s3-similarity-connected-component-0.58";
		
		return context.textFile(resultDir)
				.map(S3Score::fromString)
				.filter(i -> minThreshold <= i.getS3Score());
	}
	
	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("CreateDocumentPairsToJudge")
				.build()
				.defaultHelp(true)
				.description("Random Sample of document-pairs to judge.");
		
		addCollectionsToArgparser(parser);
		
		parser.addArgument("--startOnBucket")
			.choices(Arrays.asList("0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"))
			.setDefault("0.3")
			.help("Specify the minimum s3-score used for the creation of the sample.");
		
		parser.addArgument("--pairsPerBucket")
			.type(Integer.class)
			.setDefault(15)
			.help("How much random-samples per 0.1-bucket?");
		
		return parser.parseArgsOrFail(args);
	}
	
	@Override
	public String jobName() {
		return "create-doc-pairs";
	}
	
	private int pairsPerBucket() {
		return parsedArgs.getInt("pairsPerBucket");
	}
	
	private double startingBucket() {
		return Double.parseDouble(parsedArgs.getString("startOnBucket"));
	}
}
