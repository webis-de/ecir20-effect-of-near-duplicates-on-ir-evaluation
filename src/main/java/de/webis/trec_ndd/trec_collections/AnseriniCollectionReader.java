package de.webis.trec_ndd.trec_collections;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;

import com.google.common.collect.Iterators;

import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.OtherCollections;
import io.anserini.collection.ClueWeb09Collection;
import io.anserini.collection.ClueWeb12Collection;
import io.anserini.collection.CommonCrawlCollection;
import io.anserini.collection.DocumentCollection;
import io.anserini.collection.Segment;
import io.anserini.collection.SegmentProvider;
import io.anserini.collection.SourceDocument;
import io.anserini.index.IndexCollection;
import io.anserini.index.IndexCollection.Args;
import io.anserini.index.IndexCollection.Counters;
import io.anserini.index.generator.LuceneDocumentGenerator;
import lombok.Data;
import lombok.SneakyThrows;

@Data
@SuppressWarnings("serial")
public class AnseriniCollectionReader<T extends SourceDocument> implements CollectionReader, Serializable {
	private final CollectionConfiguration config;
	
	@Override
	public List<CollectionDocument> extractJudgedRawDocumentsFromCollection() {
		List<CollectionDocument> ret = new LinkedList<>();

		for(String segmentPath : segmentPaths()) {
			judgedCollectionDocumentsInPath(segmentPath).forEachRemaining(ret::add);
		}

		return ret;
	}
	
	@Override
	public List<CollectionDocument> extractRunFileDocumentsFromsCollection() {
		List<CollectionDocument> ret = new LinkedList<>();

		for(String segmentPath : segmentPaths()) {
			documentsFromRunFilesInPath(segmentPath).forEachRemaining(ret::add);
		}

		return ret;
	}

	@Override
	public List<CollectionDocument> extractRawDocumentsFromCollection() {
		List<CollectionDocument> ret = new LinkedList<>();

		for(String segmentPath : segmentPaths()) {
			collectionDocumentsInPath(segmentPath).forEachRemaining(ret::add);
		}

		return ret;
	}

	@SneakyThrows
	public List<String> segmentPaths() {
		return documentCollection().getFileSegmentPaths()
				.stream()
				.map(p -> p.toAbsolutePath().toString())
				.collect(Collectors.toList());
	}
	
	public Iterator<DocumentHash> documentHashIterator(String segmentPath) {
		Iterator<CollectionDocument> iter = collectionDocumentsInPath(segmentPath);
		
		return Iterators.transform(iter, d -> new DocumentHash(d));
	}
	
	public Iterator<CollectionDocument> judgedCollectionDocumentsInPath(String segmentPath) {
		Set<String> judgedDocumentIds = config.judgedDocumentIds();
		
		return Iterators.filter(iteratorForCollectionDocumentsInPath(segmentPath, doc -> judgedDocumentIds.contains(documentId(doc))), i -> i != null);
	}
	
	public Iterator<CollectionDocument> documentsFromRunFilesInPath(String segmentPath) {
		Set<String> documentIdsInRunFiles = config.documentIdsInRunFiles();
		
		return documentsFromRunFilesInPath(segmentPath, documentIdsInRunFiles);
	}
	
	public Iterator<CollectionDocument> documentsFromRunFilesInPath(String segmentPath, Set<String> documentIdsInRunFiles) {
		return Iterators.filter(iteratorForCollectionDocumentsInPath(segmentPath, doc -> documentIdsInRunFiles.contains(documentId(doc))), i -> i != null);
	}
	
	public Iterator<CollectionDocument> collectionDocumentsInPath(String segmentPath) {
		return Iterators.filter(iteratorForCollectionDocumentsInPath(segmentPath, null), i -> i != null);
	}
	
	@SneakyThrows
	private Iterator<CollectionDocument> iteratorForCollectionDocumentsInPath(String segmentPath, Function<Document, Boolean> keepDocument) {
		SegmentProvider<T> collection = documentCollection();
		
		return new AnseriniCollectionSegmentIterator<>(
				collection, 
				documentGenerator(),
				collection.createFileSegment(Paths.get(segmentPath)),
				segmentPath,
				keepDocument
		);
	}

	@Data
	private static class AnseriniCollectionSegmentIterator<T extends SourceDocument>
			implements Iterator<CollectionDocument> {
		private final SegmentProvider<T> collection;
		private final LuceneDocumentGenerator<T> generator;
		private final Segment<T> segment;
		private final String segmentPath;
		private final Function<Document, Boolean> keepDocument;

		@Override
		public boolean hasNext() {
			return segment.hasNext();
		}

		@Override
		public CollectionDocument next() {
			T document = segment.next();
			if (document == null || !document.indexable()) {
				return null;
			}
			
			Document doc = generator.createDocument(document);
			if (doc == null || (keepDocument != null && Boolean.FALSE.equals(keepDocument.apply(doc)))) {
				return null;
			} else {
				CollectionDocument ret = CollectionDocument.fromLuceneDocument(doc);
				ret.setUrl(extractUrlIfPossible(document));
				return ret;
			}
		}
	}
	
	private static URL extractUrlIfPossible(SourceDocument document) {
		try {
			return extractUrlOrFail(document);
		} catch (Exception e) {
			return null;
		}
	}

	private static URL extractUrlOrFail(SourceDocument document) throws Exception {
		if (document instanceof ClueWeb09Collection.Document) {
			return new URL(((ClueWeb09Collection.Document) document).getURL());
		} else if (document instanceof ClueWeb09Collection.Document) {
			return new URL(((ClueWeb12Collection.Document) document).getURL());
		}
		
		return null;
	}
	
	@SneakyThrows
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private SegmentProvider<T> documentCollection()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		SegmentProvider<T> ret = (SegmentProvider) Class.forName("io.anserini.collection." + args().collectionClass)
				.newInstance();
		((DocumentCollection) ret).setCollectionPath(Paths.get(config.getPathToCollection()));

		if(ret instanceof CommonCrawlCollection) {
			((CommonCrawlCollection) ret).setS3Files(((OtherCollections) config).getS3Files());
		}
		
		return ret;
	}

	@SneakyThrows
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private LuceneDocumentGenerator<T> documentGenerator() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, Exception {
		Args args = args();
		Class<?> clazz = Class.forName("io.anserini.index.generator." + args.generatorClass);

		return (LuceneDocumentGenerator) clazz.getDeclaredConstructor(Args.class, Counters.class).newInstance(args,
				new IndexCollection(args).new Counters());
	}

	private Args args() {
		Args ret = new Args();
		ret.generatorClass = config.getDocumentGenerator();
		ret.collectionClass = config.getCollectionType();
		ret.index = "-index";
		ret.input = config.getPathToCollection();

		return ret;
	}

	static String documentId(Document doc) {
		return doc.getField(LuceneDocumentGenerator.FIELD_ID).stringValue();
	}
}
