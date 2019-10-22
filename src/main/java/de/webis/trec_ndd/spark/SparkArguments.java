package de.webis.trec_ndd.spark;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.ChunkSelectionStrategy;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.spark.SparkGroupByFingerprint.DocumentHashGroupKey;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public interface SparkArguments extends Runnable {
	public static List<String> choices(Enum<?>[] e) {
		return Stream.of(e)
				.map(t -> t.toString())
				.collect(Collectors.toList());
	}
	
	public Namespace getParsedArgs();
	
	public String jobName();
	
	default void addCollectionToArgparser(ArgumentParser parser) {
		parser.addArgument("-c", "--collection")
			.choices(choices(TrecCollections.values()))
			.required(Boolean.TRUE)
			.help("Specify the collection to use.");
	}
	
	default void addCollectionsToArgparser(ArgumentParser parser) {
		parser.addArgument("--collections")
			.choices(choices(TrecCollections.values()))
			.nargs("+")
			.required(Boolean.TRUE)
			.help("Specify the collection(s) to use.");
	}

	default void addDocumentSelectionToArgparser(ArgumentParser parser) {
		parser.addArgument("--documentSelection")
			.choices(choices(DocumentSelectionStrategy.values()))
			.required(Boolean.TRUE)
			.help("Specify the document selection strategy to use.");
	}
	
	default CollectionConfiguration collection() {
		return TrecCollections.valueOf(getParsedArgs().getString("collection"));
	}
	
	default ChunkSelectionStrategy chunkSelectionStrategy() {
		return ChunkSelectionStrategy.valueOf(getParsedArgs().getString("chunkSelection"));
	}
	
	default DocumentSelectionStrategy documentSelection() {
		return DocumentSelectionStrategy.valueOf(getParsedArgs().getString("documentSelection"));
	}
	
	default String s3Threshold() {
		return getParsedArgs().getString("threshold");
	}
	
	default String collectionName() {
		return collectionName(collection());
	}
	
	default String collectionName(CollectionConfiguration config) {
		return config instanceof TrecCollections ? ((TrecCollections) config).toString() : config.getClass().getSimpleName();
	}
	
	default JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(jobName());

		return new JavaSparkContext(conf);
	}
	
	default void addSimilarityToArgparser(ArgumentParser parser) {
		parser.addArgument("-s", "--similarity")
			.choices(choices(SparkGroupByFingerprint.DocumentHashGroupKey.values()))
			.required(Boolean.TRUE)
			.help("Specifies the document-groups that are used as near-duplicates.");
	}
	
	default void addS3Threshold(ArgumentParser parser) {
		parser.addArgument("--threshold")
			.required(Boolean.TRUE)
			.type(String.class)
			.setDefault("0.58")
			.help("Specify the s3 threshold to use.");
	}
	
	default DocumentHashGroupKey documentSimilarity() {
		return SparkGroupByFingerprint.DocumentHashGroupKey.valueOf(getParsedArgs().getString("similarity"));
	}
	
	default List<TrecCollections> collections() {
		return getParsedArgs().<String>getList("collections").stream()
				.map(TrecCollections::valueOf)
				.collect(Collectors.toList());
	}
	
	default void addChunkSelectionToArgparser(ArgumentParser parser) {
		parser.addArgument("--chunkSelection")
			.choices(choices(ChunkSelectionStrategy.values()))
			.required(Boolean.TRUE)
			.help("Specify the chunk selection strategy to use.");
	}
}
