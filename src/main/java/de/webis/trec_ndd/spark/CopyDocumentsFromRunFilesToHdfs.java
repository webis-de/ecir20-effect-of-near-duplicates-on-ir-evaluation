package de.webis.trec_ndd.spark;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.Getter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class CopyDocumentsFromRunFilesToHdfs implements SparkArguments {
	
	@Getter
	private final Namespace parsedArgs;
	
	private CopyDocumentsFromRunFilesToHdfs(String[] args) {
		this.parsedArgs = parseArguments(args);
	}
	
	@Override
	public void run() {
		DocumentSelectionStrategy selectionStrategy = documentSelection();
		AnseriniCollectionReader<Document> acr = new AnseriniCollectionReader<Document>(collection());
		List<String> segmentPaths = acr.segmentPaths();

		try (JavaSparkContext context = context()) {
			JavaRDD<CollectionDocument> documents = context.parallelize(segmentPaths)
				.flatMap(s -> selectionStrategy.getDocumentsInSegmentPath().apply(acr).apply(s));
			
			documents.saveAsTextFile(jobName());
		}
	}
	
	public static void main(String[] args) {
		new CopyDocumentsFromRunFilesToHdfs(args).run();
	}
	
	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("CopyDocumentsToHdfs")
			.build()
			.defaultHelp(true)
			.description("Copy all selected documents to hdfs for faster access.");

		addCollectionToArgparser(parser);
		addDocumentSelectionToArgparser(parser);

		return parser.parseArgsOrFail(args);
	}
	
	public String jobName() {
		return jobName(collectionName(), documentSelection());
	}
	
	public static String jobName(String collectionName, DocumentSelectionStrategy documentSelection) {
		if(DocumentSelectionStrategy.ALL.equals(documentSelection)) {
			throw new RuntimeException("It is too expensive to use document-selection: " + documentSelection.name());
		}
		
		return "trec-docs-in-" + documentSelection.name().toLowerCase() + "-for-" + collectionName.toLowerCase();
	}
}
