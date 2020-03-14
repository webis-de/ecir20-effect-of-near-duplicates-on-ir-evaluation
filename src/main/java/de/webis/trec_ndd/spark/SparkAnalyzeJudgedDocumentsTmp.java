package de.webis.trec_ndd.spark;

import java.util.ArrayList;
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
			JavaRDD<CollectionDocument> cw09 = context.textFile("trec-docs-in-judged-for-clueweb09")
					.map(i -> CollectionDocument.fromString(i));
			JavaRDD<CollectionDocument> cw12 = context.textFile("trec-docs-in-judged-for-clueweb12")
					.map(i -> CollectionDocument.fromString(i));
			
			cw09.union(cw12)
				.flatMap(i -> bla(i))
				.groupBy(i -> i._1)
				.map(i -> describe(i))
				.saveAsTextFile("tmp-analysis-of-judged-cw09-cw12-docs");
			
		}
	}

	@SneakyThrows
	private static String describe(Tuple2<String, Iterable<Tuple2<String, Set<Word8Gramm>>>> group) {
		int docCount = 0;
		Set<Word8Gramm> nGramms = new HashSet<>();
		List<Integer> nGrammCounts = new ArrayList<>();
		Iterator<Tuple2<String, Set<Word8Gramm>>> iter = group._2.iterator();
		
		while(iter.hasNext()) {
			Tuple2<String, Set<Word8Gramm>> t = iter.next();
			docCount++;
			nGramms.addAll(t._2);
			nGrammCounts.add(t._2.size());
		}
		
		Map<String, Object> ret = new HashMap<>();
		ret.put("topic", group._1);
		ret.put("doc-count", docCount);
		ret.put("maxNGrammCountPerDoc", nGrammCounts.stream().mapToInt(i -> i).max().getAsInt());
		ret.put("nGrammCount", nGramms.size());
		ret.put("avgNGrammCountPerTopic", nGrammCounts.stream().mapToInt(i -> i).average().getAsDouble());
		
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
