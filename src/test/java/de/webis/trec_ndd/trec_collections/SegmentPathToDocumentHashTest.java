package de.webis.trec_ndd.trec_collections;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

public class SegmentPathToDocumentHashTest {

	@Test
	public void approveTransformationForRobustCollectionWithSingleSegment() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/robust", TrecCollections.ROBUST_TBD);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForClueweb09CollectionWithSingleSegment() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/clueweb09-sample", TrecCollections.CLUEWEB09);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForClueweb09CollectionWithMultipleSegments() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/clueweb09-sample-2", TrecCollections.CLUEWEB09);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForGOV1CollectionWithMultipleSegments() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/gov1-sample", TrecCollections.GOV1);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForGOV2CollectionWithMultipleSegments() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/gov2-sample", TrecCollections.GOV2);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForCore2018CollectionWithSingleSegments() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/core-2018-sample", TrecCollections.CORE2018);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForCore2017CollectionWithMultipleSegments() {
		CollectionConfiguration conf = SegmentPathToDocumentTextTest.collectionConfiguration("src/test/resources/data/core-2017-sample", TrecCollections.CORE2017);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<DocumentHash> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		Collections.sort(collectionDocuments, (a,b) -> a.getId().compareTo(b.getId()));
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	private static List<DocumentHash> mapAllSegmentPathsToDocumentRepresentation(AnseriniCollectionReader<?> collectionReader) {
		return collectionReader.segmentPaths().stream()
				.map(i -> collectionReader.documentHashIterator(i))
				.map(Lists::newArrayList)
				.flatMap(List::stream)
				.collect(Collectors.toList());
	}
}
