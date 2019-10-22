package de.webis.trec_ndd.util;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import com.google.common.collect.Streams;

import de.webis.trec_ndd.spark.SparkDeduplicateAndEvaluate;
import de.webis.trec_ndd.util.PostCorrelationUtil.Post;
import lombok.SneakyThrows;

public class RankCorrelation {

	public static double kendallTau(List<String> a, List<String> b) {
		Map<String, Double> dictionary = dictionary(a, b);
		List<Post> ap = a.stream().map(i -> new Post().setId(i)).collect(Collectors.toList()); 
		List<Post> bp = b.stream().map(i -> new Post().setId(i)).collect(Collectors.toList());
		
		return PostCorrelationUtil.intersectionKendallTauCorrelation(ap, bp);
//		return new SpearmansCorrelation().correlation(
//			data(a, dictionary),
//			data(b, dictionary)
//		);
	}
	
	private static Map<String, Double> dictionary(List<String> a, List<String> b) {
		Set<String> ret = new HashSet<>(a);
		ret.addAll(b);
		
		return Streams.mapWithIndex(ret.stream().sorted(), (str, i) -> Pair.of(str, Double.valueOf((double)i)))
			.collect(Collectors.toMap(Pair::getKey, Pair::getValue));
	}

	
	private static double[] data(List<String> list, Map<String, Double> dictionary) {
		return list.stream()
				.mapToDouble(dictionary::get)
				.toArray();
	}

	public static double sortListsAndCalculateKendallTau(List<Pair<String, Double>> a, List<Pair<String, Double>> b) {
		return kendallTau(sort(a), sort(b));
	}
	
	private static List<String> sort(List<Pair<String, Double>> ret) {
		return ret.stream()
				.sorted(ScoringChanges.trecStyleComperator())
				.map(Pair::getLeft)
				.collect(Collectors.toList());
	}

	public static double sortedKendallTauAtK(List<Pair<String, Double>> a, List<Pair<String, Double>> b, int k) {
		return kendallTauAtK(sort(a), sort(b), k);
	}
	
	
	public static double kendallTauAtK(List<String> a, List<String> b, int k) {
		List<String> tmpA = a.stream().limit(k).collect(Collectors.toList());
		List<String> tmpB = b.stream().limit(k).collect(Collectors.toList());
		
		tmpA = expandFirstListWithEntriesFromSecond(tmpA, tmpB);
		tmpB = expandFirstListWithEntriesFromSecond(tmpB, tmpA);
		
		return kendallTau(tmpA, tmpB);
	}
	
	@SneakyThrows
	public static void main(String[] args) {
		String bla = "/home/maik/workspace/wstud-thesis-bittner/intermediate-results/gov-evaluation.jsonl";
		List<String>ret = new ArrayList<>();
		
		for(String json : Files.readAllLines(Paths.get(bla))) {
			ret.add(SparkDeduplicateAndEvaluate.fromString(json));
		}
		
		Files.write(Paths.get(bla), ret);
	}
	
	private static List<String> expandFirstListWithEntriesFromSecond(List<String> a, List<String> b)
	{
		List<String> ret = new ArrayList<>(a);
		List<String> tmp = new ArrayList<>(b);
		tmp.removeAll(ret);
		
		ret.addAll(tmp);
		
		return ret;
	}

}
