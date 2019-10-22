package de.webis.trec_ndd.spark;

import java.io.Serializable;
import java.util.function.Function;

import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class SparkGroupByFingerprint implements SparkArguments {
	
	@Getter
	private final Namespace parsedArgs;
	
	private SparkGroupByFingerprint(String[] args) {
		this.parsedArgs = parseArguments(args);
	}
	
	@Override
	public void run() {
		String sourceDirectory = SparkHashDataset.jobName(collection(), documentSelection());
		
		try (JavaSparkContext sc = context()) {
			for(DocumentHashGroupKey groupKey : DocumentHashGroupKey.values()) {
				sc.textFile(sourceDirectory)
					.map(line -> jsonLineToDocumentTuple(line, groupKey))
					.groupBy(Tuple2::_1)
					.map(group -> new DocumentGroup(group)).filter(dg -> dg.ids.size() > 1)
					.saveAsTextFile(resultDir(collection(), documentSelection(), groupKey));
			}
		}
	}
	
	public static void main(String[] args) {
		new SparkGroupByFingerprint(args).run();
	}
	
	@SneakyThrows
	private static Tuple2<String, String> jsonLineToDocumentTuple(String jsonLine, DocumentHashGroupKey groupKey) {
		ObjectMapper mapper = new ObjectMapper();
		DocumentHash document = mapper.readValue(jsonLine, DocumentHash.class);
		String hash = groupKey.getDocumentToHash().apply(document);
		
		return new Tuple2<>(hash, document.getId());
	}

	public String jobName() {
		return jobName(collection(), documentSelection());
	}
	
	public static String jobName(CollectionConfiguration config, DocumentSelectionStrategy documentSelection) {
		String collection = config instanceof TrecCollections ? ((TrecCollections) config).toString() : config.getClass().getSimpleName();
		
		return "trec-fingerprint-groups-" + collection.toLowerCase() + "-" +documentSelection.name().toLowerCase();
	}

	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("SparkFingerprintGroups")
			.build()
			.defaultHelp(true)
			.description("Group document representations by hashes.");

		addCollectionToArgparser(parser);
		addDocumentSelectionToArgparser(parser);
		
		return parser.parseArgsOrFail(args);
	}
	
	public static String resultDir(CollectionConfiguration config, DocumentSelectionStrategy documentSelection, DocumentHashGroupKey groupKey) {
		return resultDir(config, documentSelection, groupKey.name());
	}
	
	public static String resultDir(CollectionConfiguration config, DocumentSelectionStrategy documentSelection, String similarity) {
		return resultDir(jobName(config, documentSelection), similarity);
	}
	
	private static String resultDir(String jobName, String similarity) {
		return (jobName + "/" + similarity).toLowerCase();
	}
	
	@Getter
	@AllArgsConstructor
	public static enum DocumentHashGroupKey implements Serializable {
		TEXT_PROFILE_SIGNATURE(d -> d.getTextProfileSignature()),
		MD5_CONTENT(d -> d.getMd5()),
		CANONICALIZED_TEXT_PROFILE_SIGNATURE(d -> d.getFullyTextProfileSignature()),
		CANONICALIZED_MD5_CONTENT(d -> d.getFullyCanonicalizedMd5());
		
		private final Function<DocumentHash, String> documentToHash;
	}
}
