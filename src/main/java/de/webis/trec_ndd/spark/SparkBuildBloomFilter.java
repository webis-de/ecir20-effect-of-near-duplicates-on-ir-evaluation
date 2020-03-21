package de.webis.trec_ndd.spark;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.collect.Iterators;

import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.SneakyThrows;

public class SparkBuildBloomFilter {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> docs = context.textFile("tmp-analysis-of-judged-cw09-cw12-docs");
			
			docs.flatMap(i -> bla(i))
				.saveAsTextFile("tmp-bloom-filter");
		}
	}

	@SuppressWarnings("unchecked")
	@SneakyThrows
	private static Iterator<String> bla(String str) {
		Map<String, Object> parsed = new ObjectMapper().readValue(str, new TypeReference<Map<String, Object>>() {});
		Set<Word8Gramm> word8gramms = (Set<Word8Gramm>) parsed.get("word8gramms");
		
		return Iterators.transform(word8gramms.iterator(), i -> i.getMd5Hash());
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkBuildBloomFilter");

		return new JavaSparkContext(conf);
	}
}
