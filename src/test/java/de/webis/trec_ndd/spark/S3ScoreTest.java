package de.webis.trec_ndd.spark;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.generator.LuceneDocumentGenerator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;

public class S3ScoreTest {
	@Test
	public void testS3ScoreCalculation() {
		S3ScoreIntermediateResult intermediateS3 = new S3ScoreIntermediateResult();
		intermediateS3.setCommonNGramms(1);
		intermediateS3.setLeftMetadata(docHash(null, 2));
		intermediateS3.setRightMetadata(docHash(null, 3));
		
		Approvals.verifyAsJson(new S3Score(intermediateS3));
	}
	
	@Test
	public void testS3ScoreCalculationOnShortDocuments() {
		S3ScoreIntermediateResult intermediateS3 = new S3ScoreIntermediateResult();
		intermediateS3.setCommonNGramms(0);
		intermediateS3.setLeftMetadata(docHash("a", 0));
		intermediateS3.setRightMetadata(docHash("b", 0));
		
		S3Score s3 = new S3Score(intermediateS3);
		if(Double.isNaN(s3.getS3Score())) {
			s3.setS3Score(0.0);
		}
		
		Approvals.verifyAsJson(s3);
	}
	
	@Test
	public void testS3ScoreCalculationOnShortDocumentsWithIdenticalHash() {
		S3ScoreIntermediateResult intermediateS3 = new S3ScoreIntermediateResult();
		intermediateS3.setCommonNGramms(0);
		intermediateS3.setLeftMetadata(docHash("a", 0));
		intermediateS3.setRightMetadata(docHash("a", 0));
		
		S3Score s3 = new S3Score(intermediateS3);
		Approvals.verifyAsJson(s3);
	}
	
	@Test
	public void testS3ScoreOnRealWorldExampleWithIdenticalHash() {
		CollectionDocument a = doc("Ohio Department Of Natural Resources http://www.dnr.state.oh.us", 1);
		CollectionDocument b = doc("Ohio Department Of Natural Resources http://www.dnr.state.oh.us", 2);
		
		S3ScoreIntermediateResult intermediateS3 = new S3ScoreIntermediateResult();
		intermediateS3.setCommonNGramms(0);
		intermediateS3.setLeftMetadata(new DocumentHash(a));
		intermediateS3.setRightMetadata(new DocumentHash(b));
		
		S3Score s3 = new S3Score(intermediateS3);
		Approvals.verifyAsJson(s3);
	}
	
	public CollectionDocument doc(String docContent, int id) {
		Document doc = new Document();
		doc.add(new StoredField(LuceneDocumentGenerator.FIELD_BODY, docContent));
		doc.add(new StoredField(LuceneDocumentGenerator.FIELD_ID, String.valueOf(id)));

		return CollectionDocument.fromLuceneDocument(doc);
	}
	
	private DocumentHash docHash(String hash, int nGramms) {
		DocumentHash ret = new DocumentHash();
		ret.setFullyCanonicalizedMd5(hash);
		ret.setFullyCanonicalizedWord8GrammSetSize(nGramms);
		
		return ret;
	}
}
