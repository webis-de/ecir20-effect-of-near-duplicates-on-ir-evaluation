package de.webis.trec_ndd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class SparkHashDataset {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			documents(context)
				.saveAsTextFile("trec2020/health-misinformation-document-hashs");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("health-misinformation-spex-warc-8-gramm-index");

		return new JavaSparkContext(conf);
	}
	
	private static JavaRDD<DocumentHash> documents(JavaSparkContext context) {
		return context.textFile("trec2020/health-misinformation-collection-documents/*")
			.map(i -> CollectionDocument.fromString(i))
			.map(i -> new DocumentHash());
	}
}
