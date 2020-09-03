package de.webis.trec_ndd.spark;

import static de.webis.trec_ndd.spark.SparkDeduplicateAndEvaluate.readGroupsUsedInRunFiles;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.module.SimpleAbstractTypeResolver;
import org.codehaus.jackson.map.module.SimpleModule;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.ChunkSelectionStrategy;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.DocumentSelectionStrategy;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import de.webis.trec_ndd.spark.SparkGroupByFingerprint.DocumentHashGroupKey;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class S3ScoreOnWord8GrammIndex {
	public static void main(String[] args) {
		double threshold = 0.84;
		try (JavaSparkContext context = context()) {
			JavaRDD<S3ScoreIntermediateResult> intermediateS3 = sumCoocurrencesOfAllIndexEntries(context);
			Set<String> documentIds = extractDocumentIdsUsedInIndex(intermediateS3);
			
			JavaPairRDD<String, DocumentHash> metadata = documentMetadata(context, documentIds);
			
			intermediateS3 = joinMetadataOfLeftDocument(intermediateS3, metadata);
			intermediateS3 = joinMetadataOfRightDocument(intermediateS3, metadata);
			intermediateS3 = intermediateS3.union(retrievalEquivalentDocsWithZeroWord8Gramms(metadata, context));
			
			intermediateS3.map(i -> new S3Score(i))
				.saveAsTextFile("trec2020/health-misinformation-intermediate-unfiltered-s3-similarity-connected-component-" + threshold);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("health-misinformation-s3-score-on-word-8-gramm-index");

		return new JavaSparkContext(conf);
	}

	private static JavaRDD<S3ScoreIntermediateResult> retrievalEquivalentDocsWithZeroWord8Gramms(JavaPairRDD<String, DocumentHash> metadata, JavaSparkContext sc) {
		ArrayList<DocumentGroup> retrievalEquivalent = readGroupsUsedInRunFiles(sc, "trec2020/health-misinformation-document-fingerprint-groups-" +DocumentHashGroupKey.CANONICALIZED_MD5_CONTENT.name(), null);
		
		JavaRDD<S3ScoreIntermediateResult> ret = sc.parallelize(retrievalEquivalent)
				.flatMap(i -> shortRetrievalEquivalentDocGroupToS3IntermediateResults(i));
		
		ret = joinMetadataOfLeftDocument(ret, metadata);
		ret = joinMetadataOfRightDocument(ret, metadata);
		
		return ret.filter(i -> i.getLeftMetadata().getFullyCanonicalizedWord8GrammSetSize() == 0 && i.getRightMetadata().getFullyCanonicalizedWord8GrammSetSize() == 0);
	}

	public static Iterator<S3ScoreIntermediateResult> shortRetrievalEquivalentDocGroupToS3IntermediateResults(DocumentGroup group) {
		List<String> ids = new ArrayList<>(group.getIds());
		Collections.sort(ids);
		List<S3ScoreIntermediateResult> ret = new LinkedList<>();
		
		for(int i=0; i<ids.size() -1; i++) {
			for(int j=i+1; j<ids.size(); j++) {
				S3ScoreIntermediateResult intermediateScore = new S3ScoreIntermediateResult();
				intermediateScore.setCommonNGramms(0);
				intermediateScore.setIdPair(SymmetricPairUtil.of(ids.get(i), ids.get(j)));
				
				ret.add(intermediateScore);
			}
		}
		
		return ret.iterator();
	}
	
	@Data
	@NoArgsConstructor
	@Accessors(chain = true)
	@SuppressWarnings("serial")
	public static class S3ScoreIntermediateResult implements Serializable {
		private int commonNGramms;
		private DocumentHash leftMetadata;
		private DocumentHash rightMetadata;
		
		private Pair<String, String> idPair;
	}
	
	private static Set<String> extractDocumentIdsUsedInIndex(JavaRDD<S3ScoreIntermediateResult> rdd) {
		return rdd
			.flatMap(i -> Arrays.asList(i.getIdPair().getLeft(), i.getIdPair().getRight()).iterator())
			.collect().stream().collect(Collectors.toSet());
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> sumCoocurrencesOfAllIndexEntries(JavaSparkContext context) {
		return allIndexEntries(context)
				.flatMap(indexEntry -> SymmetricPairUtil.extractCoocurrencePairs(indexEntry).iterator())
				.groupBy(Pair::getLeft)
				.map(S3ScoreOnWord8GrammIndex::sum);
	}
	
	private static S3ScoreIntermediateResult sum(Tuple2<Pair<String, String>, Iterable<Pair<Pair<String, String>, Integer>>> v) {
		int sum = 0;
		Iterator<Pair<Pair<String, String>, Integer>> iter = v._2().iterator();
		
		while(iter.hasNext()) {
			sum += iter.next().getRight();
		}
		
		return new S3ScoreIntermediateResult()
				.setCommonNGramms(sum)
				.setIdPair(v._1());
	}

	public static String jobName(CollectionConfiguration config) {
		String collection = config instanceof TrecCollections ? ((TrecCollections) config).toString() : config.getClass().getSimpleName();

		return "trec-s3-score-" + collection.toLowerCase();
	}
	
	private static JavaPairRDD<String, DocumentHash> documentMetadata(JavaSparkContext sc, Set<String> documentIds) {
		return sc.textFile("trec2020/health-misinformation-collection-documents/*")
				.map(i -> CollectionDocument.fromString(i))
				.map(i -> new DocumentHash(i))
				.filter(doc -> documentIds.contains(doc.getId()))
				.mapToPair(d -> new Tuple2<>(d.getId(), d));
	}
	
	private static JavaRDD<Word8GrammIndexEntry> allIndexEntries(JavaSparkContext sc) {
		return sc.textFile("trec2020/health-misinformation-spex-warc-8-gramm-index/*")
				.map(Word8GrammIndexEntry::fromString);
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> joinMetadataOfLeftDocument(JavaRDD<S3ScoreIntermediateResult> intermediateS3, JavaPairRDD<String, DocumentHash> metadata) {
		JavaPairRDD<String, Tuple2<S3ScoreIntermediateResult, DocumentHash>> joined = intermediateS3
				.mapToPair(i -> new Tuple2<String, S3ScoreIntermediateResult>(i.getIdPair().getLeft(), i))
				.join(metadata);
		
		return joined.map(i -> {
			S3ScoreIntermediateResult ret = i._2._1;
			ret.setLeftMetadata(i._2._2);

			return ret;
		});
	}
	
	private static JavaRDD<S3ScoreIntermediateResult> joinMetadataOfRightDocument(JavaRDD<S3ScoreIntermediateResult> intermediateS3, JavaPairRDD<String, DocumentHash> metadata) {
		JavaPairRDD<String, Tuple2<S3ScoreIntermediateResult, DocumentHash>> joined = intermediateS3
				.mapToPair(i -> new Tuple2<String, S3ScoreIntermediateResult>(i.getIdPair().getRight(), i))
				.join(metadata);
		
		return joined.map(i -> {
			S3ScoreIntermediateResult ret = i._2._1;
			ret.setRightMetadata(i._2._2);

			return ret;
		});
	}
	
	@Data
	@NoArgsConstructor
	@SuppressWarnings("serial")
	public static class S3Score implements Serializable {
		private Pair<String, String> idPair;
		private int commonNGramms;
		private double s3Score;
		double chunksInA,
				chunksInB,
				meanChunks;
		
		private static final ObjectReader READER = reader();
		
		public S3Score(S3ScoreIntermediateResult data) {
			this.chunksInA = data.getLeftMetadata().getFullyCanonicalizedWord8GrammSetSize();
			this.chunksInB = data.getRightMetadata().getFullyCanonicalizedWord8GrammSetSize();
			this.meanChunks = new Mean().evaluate(new double[] {chunksInA, chunksInB});
			this.s3Score = data.commonNGramms/meanChunks;
			this.idPair = data.idPair;
			this.commonNGramms = data.commonNGramms;
			
			if(chunksInA <= 0.01 && chunksInB <= 0.01 
				&& data.getLeftMetadata().getFullyCanonicalizedMd5() != null 
				&& data.getLeftMetadata().getFullyCanonicalizedMd5().equals(data.getRightMetadata().getFullyCanonicalizedMd5())) {
				this.s3Score = 1.0;
			}
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static S3Score fromString(String value) {
			S3Score ret = READER.readValue(value);
			if(ret.getIdPair() != null) {
				ret.setIdPair(Pair.of(ret.getIdPair().getLeft(), ret.getIdPair().getRight()));
			}
			
			return ret;
		}
		
		private static final ObjectReader reader() {
			ObjectMapper mapper = new ObjectMapper();

			SimpleModule module = new SimpleModule("CustomModel", Version.unknownVersion());

			SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
			resolver.addMapping(Pair.class, MyDummPair.class);
			module.setAbstractTypes(resolver);
			mapper.registerModule(module);
			
			return mapper.reader(S3Score.class);
		}

		@Data
		@NoArgsConstructor
		@EqualsAndHashCode(callSuper = true)
		public static class MyDummPair<L, R> extends Pair<L, R> {

		    public L left;

		    public R right;
			
			@Override
			public R setValue(R value) {
		        final R result = getRight();
		        setRight(value);
		        return result;
			}
			
			public void setKey(L key) {
				
			}
		}
	}
}
