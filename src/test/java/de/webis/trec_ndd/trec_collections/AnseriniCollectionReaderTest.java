package de.webis.trec_ndd.trec_collections;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration.OtherCollections;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.util.S3Files;
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
	
	@Test
	public void checkMockedFilesOfMockedS3Bucket() {
		S3Files s3Files = mockFileSystemS3("src/test/resources/data/corpus-commoncrawl-main-2015-11-sample");
		List<String> expected = Arrays.asList(
			"1424936465599.34/warc/CC-MAIN-20150226074105-00336-ip-10-28-5-156.ec2.internal.warc.gz",
			"1424936459513.8/warc/CC-MAIN-20150226074059-00000-ip-10-28-5-156.ec2.internal.warc.gz"
		);
		List<String> actual = s3Files.filesInBucket().stream().map(i -> i.getKey()).collect(Collectors.toList());
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkS3FileCanReturnContent() {
		S3Files s3Files = mockFileSystemS3("src/test/resources/data/corpus-commoncrawl-main-2015-11-sample");
		
		Assert.assertNotNull(s3Files.rawContent("1424936465599.34/warc/CC-MAIN-20150226074105-00336-ip-10-28-5-156.ec2.internal.warc.gz"));
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
		CollectionConfiguration config = OtherCollections.commonCrawl_2015_02(mockFileSystemS3(pathToCollection));
		
		return new AnseriniCollectionReader<T>(config);
	}
	
	private static S3Files mockFileSystemS3(String path) {
		List<S3ObjectSummary> s3Files = Arrays.asList(
			summary("1424936465599.34/warc/CC-MAIN-20150226074105-00336-ip-10-28-5-156.ec2.internal.warc.gz"),
			summary("1424936459513.8/warc/CC-MAIN-20150226074059-00000-ip-10-28-5-156.ec2.internal.warc.gz")
		);
		
		S3Files ret = Mockito.mock(S3Files.class);
		Mockito.when(ret.filesInBucket()).thenReturn(s3Files);
		Mockito.when(ret.getBucketName()).thenReturn(path);
		Mockito.when(ret.rawContent(Mockito.anyString())).then(new Answer<InputStream>() {

			@Override
			public InputStream answer(InvocationOnMock invocation) throws Throwable {
				String key = (String) invocation.getArguments()[0];
				return Files.newInputStream(Paths.get(path + "/" + key));
			}
			
		});
		
		return ret;
	}
	
	private static S3ObjectSummary summary(String key) {
		S3ObjectSummary ret = Mockito.mock(S3ObjectSummary.class);
		Mockito.when(ret.getKey()).thenReturn(key);
		
		return ret;
	}
}
