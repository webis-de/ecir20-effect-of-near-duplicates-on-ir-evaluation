package de.webis.trec_ndd.spark;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;

import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class SparkBuild8GrammIndex implements SparkArguments {
	
	@Getter
	private final Namespace parsedArgs;
	
	private SparkBuild8GrammIndex(String[] args) {
		this.parsedArgs = parseArguments(args);
	}
	
	@Override
	public void run() {
		ChunkSelectionStrategy chunkSelection = chunkSelectionStrategy();

		try (JavaSparkContext context = context()) {
			documents(parsedArgs, context)
				.flatMap(SparkBuild8GrammIndex::documentTo8Gramms)
				.groupBy(Tuple2::_1)
				.map(Word8GrammIndexEntry::buildIndexEntry)
				.filter(c -> chunkSelection.getKeepIndexEntry().apply(c))
				.saveAsTextFile(jobName());
		}
	}
	
	public static void main(String[] args) {
		new SparkBuild8GrammIndex(args).run();
	}
	
	private JavaRDD<CollectionDocument> documents(Namespace parsedArgs, JavaSparkContext context) {
		String collection = collectionName();
		DocumentSelectionStrategy documentSelection = documentSelection();
		
		if (!DocumentSelectionStrategy.ALL.equals(documentSelection)) {
			return context.textFile(CopyDocumentsFromRunFilesToHdfs.jobName(collection, documentSelection))
				.map(CollectionDocument::fromString);
		}
		else {
			AnseriniCollectionReader<Document> acr = new AnseriniCollectionReader<Document>(collection());
			List<String> segmentPaths = acr.segmentPaths();
			
			return context.parallelize(segmentPaths)
				.flatMap(s -> documentSelection.getDocumentsInSegmentPath().apply(acr).apply(s));	
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@Accessors(chain=true)
	@SuppressWarnings("serial")
	public static class Word8GrammIndexEntry implements Serializable {
		private Word8Gramm word8Gramm;
		private List<String> documentIds;
		
		public static Word8GrammIndexEntry buildIndexEntry(Tuple2<Word8Gramm, Iterable<Tuple2<Word8Gramm, String>>> word8GrammGroup) {
			List<String> documentIds = Lists.newLinkedList(word8GrammGroup._2).stream()
					.map(Tuple2::_2)
					.collect(Collectors.toList());
			
			return new Word8GrammIndexEntry(
					word8GrammGroup._1,
					documentIds
			);
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static Word8GrammIndexEntry fromString(String str) {
			return new ObjectMapper().readValue(str, Word8GrammIndexEntry.class);
		}
	}
	
	private static Iterator<Tuple2<Word8Gramm, String>> documentTo8Gramms(CollectionDocument doc) {
		List<Tuple2<Word8Gramm, String>> ret = new LinkedList<>();
		String id = doc.getId();
		
		for(Word8Gramm nGramm :  NGramms.build8Gramms(doc.getFullyCanonicalizedContent())) {
			ret.add(new Tuple2<>(nGramm, id));
		}
		
		return ret.iterator();
	}

	@Override
	public String jobName() {
		return jobName(collection(), chunkSelectionStrategy(), documentSelection());
	}
	
	public static String jobName(CollectionConfiguration config, ChunkSelectionStrategy chunkSelectionStrategy, DocumentSelectionStrategy documentSelectionStrategy) {
		String collection = config instanceof TrecCollections ? ((TrecCollections) config).toString() : config.getClass().getSimpleName();
		String csStrategy = chunkSelectionStrategy.toString().toLowerCase();
		String docStrategy = documentSelectionStrategy.toString().toLowerCase();
		
		return "trec-8-gramm-index-" + collection.toLowerCase() + "-" + csStrategy + "-" + docStrategy;
	}
	
	@Getter
	@AllArgsConstructor
	public static enum ChunkSelectionStrategy implements Serializable {
		ALL(d -> Boolean.TRUE),
		SPEX(d -> d.getDocumentIds().size() > 1);
		
		private final Function<Word8GrammIndexEntry, Boolean> keepIndexEntry;
	}
	
	@Getter
	@AllArgsConstructor
	public static enum DocumentSelectionStrategy implements Serializable {
		ALL(acr -> acr::collectionDocumentsInPath),
		JUDGED(acr -> acr::judgedCollectionDocumentsInPath),
		RUN_FILES(acr -> documentsFromRunFilesInPath(acr));
		
		private final Function<AnseriniCollectionReader<?>, Function<String, Iterator<CollectionDocument>>> documentsInSegmentPath;
	}
	
	private static Function<String, Iterator<CollectionDocument>> documentsFromRunFilesInPath(AnseriniCollectionReader<?> collectionReader) {
		Set<String> judgedDocumentIds = collectionReader.getConfig().documentIdsInRunFiles();
		
		return s -> collectionReader.documentsFromRunFilesInPath(s, judgedDocumentIds);
	}

	private Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("Spark8GrammIndex")
				.build()
				.defaultHelp(true)
				.description("Build 8-gramm-index for all judged documents in a dataset.");

		addCollectionToArgparser(parser);
		addChunkSelectionToArgparser(parser);
		addDocumentSelectionToArgparser(parser);

		return parser.parseArgsOrFail(args);
	}
}
