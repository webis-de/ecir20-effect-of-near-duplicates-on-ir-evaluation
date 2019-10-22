package de.webis.trec_ndd.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class QueryByExampleBuilder {
	public static String esQueryByExample(String document){
		return esOrQuery(tokensInText(document));
	}
	
	public static String esQueryByExampleWithKMedianTokens(String document, int k){
		List<String> sortedTokens = new ArrayList<String>(tokensInText(document));
		Collections.sort(sortedTokens, (a,b) -> {
			try {
				return WordCounts.getWordCount(a).compareTo(WordCounts.getWordCount(b));
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(1);
				return 1;
			}
		});
		Set<String> ret = new HashSet<>();
		
		while(k > ret.size() && sortedTokens.size() > 0) {
			ret.add(sortedTokens.remove(sortedTokens.size()/2));
		}
		
		return esOrQuery(ret);
	}
	
	private static String esOrQuery(Collection<String> tokens) {
		return tokens.stream()
			.map(t -> "(body_lang.en: \"" + t + "\")")
			.collect(Collectors.joining(" OR "));
	}

	private static Set<String> tokensInText(String text) {
		List<String> ret = textToTokenList(text);
		return new HashSet<>(ret);
	}

	private static List<String> tokensInText(Analyzer analyzer, String text) throws IOException {
		List<String> ret = new LinkedList<>();
		TokenStream tokenStream = analyzer.tokenStream("", text);

		CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		while (tokenStream.incrementToken()) {
			ret.add(attr.toString());
		}

		return ret;
	}

	@SneakyThrows
	public static List<String> textToTokenList(String text) {
		Analyzer analyzer = new StandardAnalyzer();
		
		return tokensInText(analyzer, text);
	}
}
