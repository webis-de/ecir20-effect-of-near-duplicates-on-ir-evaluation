package de.webis.trec_ndd.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.math3.stat.correlation.KendallsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.commons.math3.util.Precision;

import java.util.stream.Collectors;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PostCorrelationUtil
{
	@Data
	@NoArgsConstructor
	@Accessors(chain=true)
	public static class Post {
		String id;
	}
	
	public static void main(String[] args) {
		List<Post> a = Arrays.asList(
			new Post().setId("f"),
			new Post().setId("s"),
			new Post().setId("v"),
			new Post().setId("r"),
			new Post().setId("b")
		);
		
		List<Post> b = Arrays.asList(
			new Post().setId("f"),
			new Post().setId("s"),
			new Post().setId("v"),
			new Post().setId("r"),
			new Post().setId("a")
		);
		
		System.out.println(unionSpearmanCorrelation(a,b));
	}
	
	public static double unionKendallTauCorrelation(List<Post> firstRanking, List<Post> secondRanking)
	{
		List<Post> expandedFirstRanking = expandFirstListWithEntriesFromSecond(firstRanking, secondRanking);
		List<Post> expandedSecondRanking = expandFirstListWithEntriesFromSecond(secondRanking, firstRanking);
		
		return intersectionKendallTauCorrelation(expandedFirstRanking, expandedSecondRanking);
	}
	
	public static double intersectionKendallTauCorrelation(List<Post> firstRanking, List<Post> secondRanking)
	{
		return calculateCorrelation(firstRanking, secondRanking, new KendallsCorrelation()::correlation);
	}
	
	public static double unionSpearmanCorrelation(List<Post> firstRanking, List<Post> secondRanking)
	{
		List<Post> expandedFirstRanking = expandFirstListWithEntriesFromSecond(firstRanking, secondRanking);
		List<Post> expandedSecondRanking = expandFirstListWithEntriesFromSecond(secondRanking, firstRanking);
		
		return intersectionSpearmanCorrelation(expandedFirstRanking, expandedSecondRanking);
	}
	
	public static double intersectionSpearmanCorrelation(List<Post> firstRanking, List<Post> secondRanking)
	{
		return calculateCorrelation(firstRanking, secondRanking, new SpearmansCorrelation()::correlation);
	}
	
	private static double calculateCorrelation(List<Post> firstRanking, List<Post> secondRanking, BiFunction<double[], double[], Double> correlation)
	{
		double[] firstRankingArray = buildCorrelationArrayForFirstRanking(firstRanking, secondRanking);
		double[] secondRankingArray = buildCorrelationArrayForFirstRanking(secondRanking, firstRanking);
		
		if(firstRankingArray.length == 0)
		{
			return 0;
		}
		else if(firstRankingArray.length == 1)
		{
			return Precision.equals(firstRankingArray[0], secondRankingArray[0]) ? 1 : 0;
		}
		
		return correlation.apply(firstRankingArray, secondRankingArray);
	}
	
	private static double[] buildCorrelationArrayForFirstRanking(List<Post> firstRanking, List<Post> secondRanking)
	{
		List<String> firstRankingIds = mapToIds(firstRanking);
		firstRankingIds.retainAll(mapToIds(secondRanking));
		
		List<String> indexValues = new ArrayList<>(firstRankingIds);
		indexValues.sort(String::compareTo);
		
		return indexValues.stream()
				.map(id -> firstRankingIds.indexOf(id))
				.mapToDouble(Integer::doubleValue)
				.toArray();
	}
	
	private static List<Post> expandFirstListWithEntriesFromSecond(List<Post> firstRanking, List<Post> secondRanking)
	{
		List<String> firstRankingIds = mapToIds(firstRanking);
		List<String> secondRankingIds = mapToIds(secondRanking);
		
		secondRankingIds.removeAll(firstRankingIds);
		
		if(secondRankingIds == null || secondRanking == null || secondRankingIds.size() == secondRanking.size())
		{
			return new ArrayList<>();
		}
		
		firstRankingIds.addAll(secondRankingIds);
		
		return firstRankingIds.stream()
				.map(id -> new Post().setId(id))
				.collect(Collectors.toList());
	}
	
	private static List<String> mapToIds(List<Post> posts)
	{
		if(posts == null)
		{
			return new ArrayList<>();
		}
		
		List<String> ret = posts.stream().map(Post::getId).collect(Collectors.toList());
		
		if(ret.stream().anyMatch(a -> a == null || a.isEmpty()) || ret.stream().distinct().collect(Collectors.toList()).size() != ret.size())
		{
			throw new IllegalArgumentException("The following list of posts contains duplicates or missing ids. "
					+ "I dont know how to calculate a correlation coefficient: "+ ret);
		}
		
		return ret;
	}
}
