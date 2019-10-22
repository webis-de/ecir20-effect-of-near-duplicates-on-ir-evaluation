package de.webis.trec_ndd.spark;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.Getter;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class SparkHashDataset implements SparkArguments {

	@Getter
	private final Namespace parsedArgs;
	
	private SparkHashDataset(String[] args) {
		this.parsedArgs = parseArguments(args);
	}

	@Override
	public void run() {
		try (JavaSparkContext context = context()) {
			documents(context)
				.saveAsTextFile(jobName());
		}
	}
	
	@SneakyThrows
	public static void main(String[] args) {
		new SparkHashDataset(args).run();
	}
	
	private JavaRDD<DocumentHash> documents(JavaSparkContext context) {
		CollectionConfiguration collection = collection();
		DocumentSelectionStrategy documentSelection = documentSelection();
		
		if(!DocumentSelectionStrategy.ALL.equals(documentSelection)) {
			return context.textFile(CopyDocumentsFromRunFilesToHdfs.jobName(collectionName(), documentSelection))
					.map(CollectionDocument::fromString)
					.map(DocumentHash::new);
		}
		else {
			AnseriniCollectionReader<Document> acr = new AnseriniCollectionReader<Document>(collection);
		
			List<String> segmentPaths = acr.segmentPaths();
			return context.parallelize(segmentPaths)
					.flatMap(acr::documentHashIterator);
		}
	}

	public String jobName() {
		return jobName(collection(), documentSelection());
	}

	public static String jobName(CollectionConfiguration config, DocumentSelectionStrategy documentSelection) {
		String collection = config instanceof TrecCollections ? ((TrecCollections) config).toString() : config.getClass().getSimpleName();
		
		return "trec-ndd-hashes-" + collection.toLowerCase() + "-" + documentSelection.name().toLowerCase();
	}
	
	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("SparkHashDataset")
				.build()
				.defaultHelp(true)
				.description("Build hash-document representations for all documents in a dataset.");

		addCollectionToArgparser(parser);
		addDocumentSelectionToArgparser(parser);
		
		return parser.parseArgsOrFail(args);
	}
}
