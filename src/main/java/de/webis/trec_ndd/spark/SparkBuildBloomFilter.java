package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;

import de.webis.trec_ndd.similarity.MD5;
import lombok.SneakyThrows;
import scala.Tuple3;

public class SparkBuildBloomFilter {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			BloomFilter<String> bf = bf("tmp-analysis-of-judged-cw09-cw12-docs", context);
			List<String> dummyElements = new ArrayList<>();
			for(int i=0; i<10000; i++) {
				dummyElements.add(MD5.md5hash(""+ i));
			}
			
			context.parallelize(dummyElements)
				.map(i -> tmpBla(bf, i))
				.saveAsTextFile("bloom-filter-result-tmp-test");
		}
	}
	
	private static Tuple3<String, Boolean, Long> tmpBla(BloomFilter<String> bf, String value) {
		long start = System.currentTimeMillis();
		boolean mightContain = bf.mightContain(value);
		long end = System.currentTimeMillis();
		
		return new Tuple3<>(value, mightContain, end-start);
	}
	
	private static final BloomFilter<String> bf(String dir, JavaSparkContext context) {
		Collection<String> elements = context.textFile(dir)
				.flatMap(i -> bla(i))
				.distinct()
				.collect();
		elements = new HashSet<String>(elements);
		
		BloomFilter<String> bf = BloomFilter.create(null, elements.size(), 1.0e-8);
		for(String element: elements) {
			bf.put(element);
		}
		
		return bf;
	}

	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Iterator<String> bla(String str) {
		Map<String, Object> parsed = new ObjectMapper().readValue(str, new TypeReference<Map<String, Object>>() {});
		Set<Map<String, String>> word8gramms = new HashSet<Map<String, String>>((Collection)parsed.get("word8gramms"));

		return Iterators.transform(word8gramms.iterator(), i -> i.get("md5Hash"));
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkBuildBloomFilter");

		return new JavaSparkContext(conf);
	}
}
