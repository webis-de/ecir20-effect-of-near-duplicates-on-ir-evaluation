package de.webis.trec_ndd.spark;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import avro.shaded.com.google.common.collect.Iterators;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Wither;
import scala.Tuple2;

public class SparkBuildBloomFilter {
	private static final TrecCollections COLLECTION = TrecCollections.CLUEWEB09;
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			List<Tuple2<String, BloomFilter<CharSequence>>> bf = bf("tmp-analysis-of-judged-cw09-cw12-docs", context);
			JavaRDD<CandidateDocumentForTopic> documents = collectionDocuments(context, COLLECTION, bf);
			
			documents.map(i -> i.toString())
				.saveAsTextFile("tmp-deduplication-candidates-for-cw09");
		}
	}
	
	private static final List<Tuple2<String, BloomFilter<CharSequence>>> bf(String dir, JavaSparkContext context) {
		 return context.textFile(dir)
				.map(i -> bla(i))
				.collect();
	}

	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Tuple2<String, BloomFilter<CharSequence>> bla(String str) {
		Map<String, Object> parsed = new ObjectMapper().readValue(str, new TypeReference<Map<String, Object>>() {});
		Set<Map<String, String>> word8gramms = new HashSet<Map<String, String>>((Collection)parsed.get("word8gramms"));

		Funnel<CharSequence> funnel = Funnels.stringFunnel();
		BloomFilter<CharSequence> bf = BloomFilter.create(funnel, (word8gramms.size()*2) + 10000, 1.0e-8);
		
		for(Map<String, String> word8gramm: word8gramms) {
			bf.put(word8gramm.get("md5Hash"));
		}
		
		return new Tuple2((String) parsed.get("topic"), bf);
	}
	
	private static JavaRDD<CandidateDocumentForTopic> collectionDocuments(JavaSparkContext context, CollectionConfiguration collection, List<Tuple2<String, BloomFilter<CharSequence>>> bf) {
		AnseriniCollectionReader<Document> acr = new AnseriniCollectionReader<Document>(collection);
		List<String> segmentPaths = acr.segmentPaths();

		return context.parallelize(segmentPaths)
				.flatMap(s -> calculateItForIterator(acr.collectionDocumentsInPath(s), bf))
				.flatMap(i -> i.iterator());
	}
	
	private static Iterator<List<CandidateDocumentForTopic>> calculateItForIterator(Iterator<CollectionDocument> docs, List<Tuple2<String, BloomFilter<CharSequence>>> bf) {
		return Iterators.transform(docs, i -> calculateIt(i, bf));
	}
	
	private static List<CandidateDocumentForTopic> calculateIt(CollectionDocument doc, List<Tuple2<String, BloomFilter<CharSequence>>> bf) {
		long startTime = System.currentTimeMillis();
		
		Set<Word8Gramm> word8Gramms = new HashSet<>(NGramms.build8Gramms(doc.getFullyCanonicalizedContent()));
		
		List<CandidateDocumentForTopic> ret = bf.stream()
				.map(i -> toCandidate(doc, word8Gramms, i))
				.filter(i -> i.lowerBoundOnMatching8Gramms()>= 0.4)
				.collect(Collectors.toList());
		
		long calculationTimePerDocMs = System.currentTimeMillis() - startTime;
		
		return ret.stream()
				.map(i -> i.withCalculationTimePerDocMs(calculationTimePerDocMs))
				.collect(Collectors.toList());
	}
	
	private static CandidateDocumentForTopic toCandidate(CollectionDocument doc, Set<Word8Gramm> word8Gramms, Tuple2<String, BloomFilter<CharSequence>> bf) {
		int mightMatching8Gramms = 0;
		
		for(Word8Gramm word8Gramm: word8Gramms) {
			if(bf._2.mightContain(word8Gramm.getMd5Hash())) {
				mightMatching8Gramms++;
			}
		}
		
		return new CandidateDocumentForTopic()
				.withCollection(COLLECTION.name())
				.withDocumentId(doc.getId())
				.withTopic(bf._1())
				.withWord8Gramms(word8Gramms.size())
				.withMightMatching8Gramms(mightMatching8Gramms);
	}
	
	@Data
	@Wither
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CandidateDocumentForTopic {
		private String topic, documentId, collection;
		private int word8Gramms, mightMatching8Gramms;
		private long calculationTimePerDocMs;
		
		public double lowerBoundOnMatching8Gramms() {
			return ((double) mightMatching8Gramms)/((double) word8Gramms);
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkBuildBloomFilter");

		return new JavaSparkContext(conf);
	}
}
