package de.webis.trec_ndd.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.util.SymmetricPairUtil;

public class S3SimilarityGroupTest extends SharedJavaSparkContext {
	
	private static final List<S3ScoreIntermediateResult> LARGE_SCORE_EXAMPLES = Arrays.asList(
		s3Score("A", "B", 0.3),
		s3Score("A", "C", 0.6),
		s3Score("A", "D", 0.1),
		s3Score("B", "D", 0.6),
		s3Score("E", "F", 0.6),
		s3Score("G", "F", 0.6),
		s3Score("H", "E", 0.6),
		s3Score("I", "J", 0.6),
		s3Score("K", "I", 0.6),
		s3Score("L", "I", 0.6)
	);
	
	@Test
	public void approveGroupingWithThreshold0_65() {
		JavaRDD<S3ScoreIntermediateResult> rdd = jsc().parallelize(LARGE_SCORE_EXAMPLES);
		List<DocumentGroup> actual = groupWithSpark(rdd, 0.65);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveGroupingWithThreshold0_50() {
		JavaRDD<S3ScoreIntermediateResult> rdd = jsc().parallelize(LARGE_SCORE_EXAMPLES);
		List<DocumentGroup> actual = groupWithSpark(rdd, 0.5);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveGroupingWithThreshold0_25() {
		JavaRDD<S3ScoreIntermediateResult> rdd = jsc().parallelize(LARGE_SCORE_EXAMPLES);
		List<DocumentGroup> actual = groupWithSpark(rdd, 0.25);
		
		Approvals.verifyAsJson(actual);
	}
	
	private static List<DocumentGroup> groupWithSpark(JavaRDD<S3ScoreIntermediateResult> rdd, double threshold) {
		JavaRDD<DocumentGroup> groups = GraphTest.group(rdd, threshold);
		return groups.collect().stream()
				.map(d -> {
					Collections.sort(d.ids);
					d.hash = "FIRST-DOC-IS: " + d.ids.get(0);
					return d;
				})
				.sorted((a,b) -> a.getHash().compareTo(b.getHash()))
				.collect(Collectors.toList());
	}
	
	private static S3ScoreIntermediateResult s3Score(String a, String b, double score) {
		DocumentHash docHash = new DocumentHash();
		docHash.setFullyCanonicalizedWord8GrammSetSize(10);
		
		return new S3ScoreIntermediateResult()
				.setLeftMetadata(docHash)
				.setRightMetadata(docHash)
				.setCommonNGramms((int) (score*10.0))
				.setIdPair(SymmetricPairUtil.of(a, b));
	}
}
