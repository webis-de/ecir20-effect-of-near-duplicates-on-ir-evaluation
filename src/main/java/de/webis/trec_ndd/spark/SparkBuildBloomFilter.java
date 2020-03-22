package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import de.webis.trec_ndd.similarity.MD5;
import lombok.SneakyThrows;
import scala.Tuple2;
import scala.Tuple3;

public class SparkBuildBloomFilter {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			List<Tuple2<String, BloomFilter<CharSequence>>> bf = bf("tmp-analysis-of-judged-cw09-cw12-docs", context);
			List<String> dummyElements = new ArrayList<>();
			for(int i=0; i<1000000; i++) {
				dummyElements.add(MD5.md5hash(""+ i));
			}
			
			context.parallelize(dummyElements)
				.map(i -> tmpBla(bf, i))
				.saveAsTextFile("bloom-filter-result-tmp-test");
		}
	}
	
	private static Tuple3<String, List<String>, Long> tmpBla(List<Tuple2<String, BloomFilter<CharSequence>>> bf, String value) {
		long start = System.currentTimeMillis();
		List<String> possibleMatches = new ArrayList<>();
		for(Tuple2<String, BloomFilter<CharSequence>> t: bf) {
			if(t._2.mightContain(value)) {
				possibleMatches.add(t._1());
			}
		}
		long end = System.currentTimeMillis();

		return new Tuple3<>(value, possibleMatches, end-start);
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
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkBuildBloomFilter");

		return new JavaSparkContext(conf);
	}
}
