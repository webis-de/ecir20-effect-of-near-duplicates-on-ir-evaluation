package de.webis.trec_ndd.trec_collections;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration.OtherCollections;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import io.anserini.collection.SourceDocument;

public class AnseriniCollectionReaderTest<T extends SourceDocument> {
	@Test
	public void approveTransformationOfSmallCollection() {
		CollectionReader reader = robustAnseriniCollectionReader();
		List<CollectionDocument> documents = reader.extractJudgedRawDocumentsFromCollection();

		Approvals.verifyAsJson(documents);
	}
	
	@Test
	public void approveTransformationOfSmallCluewebSampleCollection() {
		CollectionReader reader = cluewebAnseriniCollectionReader();
		List<CollectionDocument> documents = reader.extractJudgedRawDocumentsFromCollection();

		Approvals.verifyAsJson(documents);
	}
	
	@Test
	public void approveTransformationOfRunFileFilteredDocuments() {
		CollectionReader reader = core2018AnseriniCollectionReader();
		List<CollectionDocument> documents = reader.extractRunFileDocumentsFromsCollection();

		Approvals.verifyAsJson(documents);
	}
	
	@Test
	public void approveTransformationOfCommonCrawl2015SampleDocuments() {
		CollectionReader reader = commonCrawlCollectionReader();
		List<CollectionDocument> documents = reader.extractRawDocumentsFromCollection();

		Approvals.verifyAsJson(documents);
	}
	
	private static <T extends SourceDocument> CollectionReader core2018AnseriniCollectionReader() {
		String pathToCollection = "src/test/resources/data/core-2018-sample";
		List<SharedTask> sharedTasks = Arrays.asList(RankingResultParsingTest.sharedTaskWithRunFileDirectory("src/test/resources/data/artificial-sample-run-files-core-2018"));
		CollectionConfiguration config = SegmentPathToDocumentTextTest.collectionConfigurationWithSharedTasks(pathToCollection, sharedTasks, TrecCollections.CORE2018);
		
		return new AnseriniCollectionReader<T>(config);
	}

	private static <T extends SourceDocument> CollectionReader robustAnseriniCollectionReader() {
		String pathToCollection = "src/test/resources/data/robust";
		List<String> qrelResources = Arrays.asList("/data/robust-qrels.txt");
		CollectionConfiguration config = SegmentPathToDocumentTextTest.collectionConfiguration(pathToCollection, qrelResources, TrecCollections.ROBUST04);
		
		return new AnseriniCollectionReader<T>(config);
	}
	
	private static <T extends SourceDocument> CollectionReader cluewebAnseriniCollectionReader() {
		String pathToCollection = "src/test/resources/data/clueweb09-sample";
		List<String> qrelResources = Arrays.asList("/data/clueweb09-qrels.txt");
		CollectionConfiguration config = SegmentPathToDocumentTextTest.collectionConfiguration(pathToCollection, qrelResources, TrecCollections.CLUEWEB09);
		
		return new AnseriniCollectionReader<T>(config);
	}
	
	private static <T extends SourceDocument> CollectionReader commonCrawlCollectionReader() {
		String pathToCollection = "src/test/resources/data/corpus-commoncrawl-main-2015-11-sample";
		List<String> qrelResources = Collections.emptyList();
		CollectionConfiguration config = SegmentPathToDocumentTextTest.collectionConfiguration(pathToCollection, qrelResources, OtherCollections.COMMON_CRAWL_2015_02);
		
		return new AnseriniCollectionReader<T>(config);
	}
}
