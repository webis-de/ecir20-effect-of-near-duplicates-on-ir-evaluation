package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.ImmutableList;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import scala.reflect.ClassTag;

import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

public class GraphTest {
	public static JavaRDD<DocumentGroup> group(JavaRDD<S3ScoreIntermediateResult> rdd, double threshold) {
		JavaRDD<S3Score> scores = rdd.map(i -> new S3Score(i))
			.filter(score -> score != null && score.getS3Score() > threshold);
		
		Map<String, Long> docToVertex = documentIdToVertexId(scores);
		Map<Long, String> vertexToDoc = revertMap(docToVertex);
		
		JavaRDD<Edge<Double>> edges = scores
			.flatMap(i -> {
				Long a = docToVertex.get(i.getIdPair().getLeft());
				Long b = docToVertex.get(i.getIdPair().getRight());
				
				return Arrays.asList(
					new Edge<Double>(a, b, i.getS3Score()),
					new Edge<Double>(b, a, i.getS3Score())	
				).iterator();
		});
		
		ClassTag<String> STRING_TAG = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Double> DOUBLE_TAG = scala.reflect.ClassTag$.MODULE$.apply(Double.class);

		Graph<String, Double> graph = Graph.fromEdges(edges.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), STRING_TAG, DOUBLE_TAG);

		JavaRDD<Pair<String, Long>> b = org.apache.spark.graphx.lib.ConnectedComponents.run(graph, STRING_TAG, DOUBLE_TAG).vertices().toJavaRDD()
				.map(i -> Pair.<String, Long>of(vertexToDoc.get((Long)i._1), (Long) i._2));
		
		return b.groupBy(i -> i.getRight())
				.map(group -> new DocumentGroup()
						.setHash(String.valueOf(group._1))
						.setIds(new ArrayList<>(ImmutableList.copyOf(group._2).stream().map(Pair::getLeft).collect(Collectors.toList()))));
	}
	
	private static Map<String, Long> documentIdToVertexId(JavaRDD<S3Score> scores) {
		return scores
			.flatMap(s3Score -> Arrays.asList(s3Score.getIdPair().getLeft(), s3Score.getIdPair().getRight()).iterator())
			.groupBy(i-> i)
			.map(i -> i._1)
			.zipWithUniqueId()
			.collect().stream()
			.collect(Collectors.toMap(i -> i._1, i -> i._2));
	}
	
	private static <K, V> Map<V, K> revertMap(Map<K,V> m) {
		return m.entrySet().stream()
				.collect(Collectors.toMap(i-> i.getValue(), i-> i.getKey()));
	}
}
