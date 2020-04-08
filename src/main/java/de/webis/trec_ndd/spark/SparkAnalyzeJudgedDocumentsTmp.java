package de.webis.trec_ndd.spark;

import java.util.Arrays;
import java.util.HashMap;
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


import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkAnalyzeJudgedDocumentsTmp {
	private static final List<TrecSharedTask> TRACKS = Arrays.asList(TrecSharedTask.WEB_2009, TrecSharedTask.WEB_2010,
			TrecSharedTask.WEB_2011, TrecSharedTask.WEB_2012, TrecSharedTask.WEB_2013, TrecSharedTask.WEB_2014);

	private static final Map<String, Set<String>> DOC_TO_TOPICS = docToTopics();

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<CollectionDocument> allJudgedDocuments = docsInTextFile("trec-docs-in-judged-for-clueweb09", context)
					.union(docsInTextFile("trec-docs-in-judged-for-clueweb12", context));
			
			allJudgedDocuments
				.flatMap(i -> bla(i))
				.groupBy(i -> i._1())
				.map(i -> describe(i))
				.saveAsTextFile("tmp-analysis-of-judged-cw09-cw12-docs");
		}
	}
	
	private static JavaRDD<CollectionDocument> docsInTextFile(String textFile, JavaSparkContext context) {
		return context.textFile(textFile)
				.map(i -> CollectionDocument.fromString(i));
	}

	@SneakyThrows
	private static String describe(Tuple2<String, Iterable<Tuple2<String, Set<Word8Gramm>>>> group) {
		Map<String, Object> ret = new HashMap<>();
		Set<Word8Gramm> word8gramms = new HashSet<>();
		Set<String> documents = new HashSet<>();
		
		Iterator<Tuple2<String, Set<Word8Gramm>>> iter = group._2.iterator();
		
		while(iter.hasNext()) {
			Tuple2<String, Set<Word8Gramm>> t = iter.next();
			documents.add(t._1());
			word8gramms.addAll(t._2());
		}
		
		ret.put("topic", group._1);
		ret.put("documents", documents);
		ret.put("word8gramms", word8gramms);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	private static Iterator<Tuple2<String, Set<Word8Gramm>>> bla(CollectionDocument doc) {
		Set<String> topics = DOC_TO_TOPICS.get(doc.getId());

		if (topics.isEmpty()) {
			throw new RuntimeException("Handle this for document " + doc.getId());
		}

		Set<Word8Gramm> ret = new HashSet<>(NGramms.build8Gramms(doc.getFullyCanonicalizedContent()));
		
		return topics.stream()
			.map(i -> new Tuple2<>(i, ret))
			.collect(Collectors.toList())
			.iterator();
	}

	private static Map<String, Set<String>> docToTopics() {
		Map<String, Set<String>> ret = new HashMap<>();
		for (SharedTask task : TRACKS) {
			Set<String> topics = task.documentJudgments().getData().keySet();

			for (String topic : topics) {
				Set<String> judgedDocs = task.documentJudgments().getData().get(topic).keySet();
				for (String doc : judgedDocs) {
					if (!ret.containsKey(doc)) {
						ret.put(doc, new HashSet<>());
					}

					ret.get(doc).add(topic);
				}
			}
		}

		return ret;
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("SparkAnalyzeJudgedDocumentsTmp");

		return new JavaSparkContext(conf);
	}
}
