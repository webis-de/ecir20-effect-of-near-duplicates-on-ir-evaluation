package de.webis.trec_ndd.trec_collections;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

public class SegmentPathToDocumentTextTest {

	@Test
	public void approveTransformationForRobustCollectionWithSingleSegment() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/robust", TrecCollections.ROBUST_TBD);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForClueweb09CollectionWithSingleSegment() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/clueweb09-sample", TrecCollections.CLUEWEB09);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForClueweb09CollectionWithMultipleSegments() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/clueweb09-sample-2", TrecCollections.CLUEWEB09);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForGOV1CollectionWithMultipleSegments() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/gov1-sample", TrecCollections.GOV1);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForGOV2CollectionWithMultipleSegments() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/gov2-sample", TrecCollections.GOV2);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForCore2018CollectionWithSingleSegments() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/core-2018-sample", TrecCollections.CORE2018);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	@Test
	public void approveTransformationForCore2017CollectionWithMultipleSegments() {
		CollectionConfiguration conf = collectionConfiguration("src/test/resources/data/core-2017-sample", TrecCollections.CORE2017);
		AnseriniCollectionReader<?> collectionReader = new AnseriniCollectionReader<>(conf);
		List<CollectionDocument> collectionDocuments = mapAllSegmentPathsToDocumentRepresentation(collectionReader);
		Collections.sort(collectionDocuments, (a,b) -> a.getId().compareTo(b.getId()));
		
		Approvals.verifyAsJson(collectionDocuments);
	}
	
	private static List<CollectionDocument> mapAllSegmentPathsToDocumentRepresentation(AnseriniCollectionReader<?> collectionReader) {
		return collectionReader.segmentPaths().stream()
				.map(i -> collectionReader.collectionDocumentsInPath(i))
				.map(Lists::newArrayList)
				.flatMap(List::stream)
				.collect(Collectors.toList());
	}
	
	public static CollectionConfiguration collectionConfiguration(String pathToCollection, TrecCollections collection) {
		return collectionConfiguration(pathToCollection, Collections.emptyList(), collection);
	}
	
	public static CollectionConfiguration collectionConfiguration(String pathToCollection, List<String> qrelResources, TrecCollections collection ) {
		List<SharedTask> sharedTasks = qrelResources.stream()
				.map(r -> sharedTaskWithQrelResource(r))
				.collect(Collectors.toList());
		
		return collectionConfigurationWithSharedTasks(pathToCollection, sharedTasks, collection);
	}

	@SuppressWarnings("serial")
	public static CollectionConfiguration collectionConfigurationWithSharedTasks(String pathToCollection, List<SharedTask> sharedTasks, TrecCollections collection ) {
		return new CollectionConfiguration() {
			@Override
			public String getPathToCollection() {
				return pathToCollection;
			}

			@Override
			public String getCollectionType() {
				return collection.getCollectionType();
			}

			@Override
			public String getDocumentGenerator() {
				return collection.getDocumentGenerator();
			}

			@Override
			public List<SharedTask> getSharedTasks() {
				return sharedTasks;
			}
		};
	}

	
	private static SharedTask sharedTaskWithQrelResource(String qrelResource) {
		SharedTask ret = Mockito.mock(SharedTask.class);
		Mockito.when(ret.getQrelResource()).thenReturn(qrelResource);
		
		return ret;
	}
}
