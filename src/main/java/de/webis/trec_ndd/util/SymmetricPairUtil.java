package de.webis.trec_ndd.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SymmetricPairUtil {

	public static <T extends Comparable<T>> Pair<T, T> of(T a, T b) {
		if (a != null && (b == null || a.compareTo(b) > 0)) {
			return Pair.of(b, a);
		}
		return Pair.of(a, b);
	}

	public static List<Pair<Pair<String, String>, Integer>> extractCoocurrencePairs(Word8GrammIndexEntry input) {
		if (input == null || input.getDocumentIds() == null) {
			return Collections.emptyList();
		}
		
		List<Pair<Pair<String, String>, Integer>> ret = new LinkedList<>();
		Set<Pair<String, String>> pairsSeen = new HashSet<>();
		
		for(int i=1; i< input.getDocumentIds().size(); i++) {
			for(int j=i;j< input.getDocumentIds().size(); j++) {
				Pair<String, String> pair = of(input.getDocumentIds().get(i-1), input.getDocumentIds().get(j));
				
				if(!pairsSeen.contains(pair) && !StringUtils.equals(pair.getLeft(), pair.getRight())) {
					ret.add(Pair.of(pair, 1));
					pairsSeen.add(pair);
				}
			}
		}
		
		return ret;
	}
}
