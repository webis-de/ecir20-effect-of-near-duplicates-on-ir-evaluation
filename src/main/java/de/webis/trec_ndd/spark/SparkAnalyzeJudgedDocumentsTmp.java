package de.webis.trec_ndd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class SparkAnalyzeJudgedDocumentsTmp {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<CollectionDocument> cw09 = context.textFile("trec-docs-in-judged-for-clueweb09")
					.map(i -> CollectionDocument.fromString(i));
			JavaRDD<CollectionDocument> cw12 = context.textFile("trec-docs-in-judged-for-clueweb12")
					.map(i -> CollectionDocument.fromString(i));
			
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkAnalyzeJudgedDocumentsTmp");

		return new JavaSparkContext(conf);
	}
}
