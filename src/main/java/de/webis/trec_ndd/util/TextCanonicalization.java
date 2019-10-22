package de.webis.trec_ndd.util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.tartarus.snowball.ext.PorterStemmer;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TextCanonicalization {
	public static List<String> fullCanonicalization(String text) {
		
		return level5Canonicalization(text).stream()
				.map(TextCanonicalization::stem)
				.collect(Collectors.toList());
	}
	
	private static String stem(String token) {
		PorterStemmer stemmer = new PorterStemmer();
		stemmer.setCurrent(token);
		stemmer.stem();
		
		return stemmer.getCurrent();
	}
	
	public static List<String> level5Canonicalization(String text) {
		if (text == null) {
			return Collections.emptyList();
		}
		
		return QueryByExampleBuilder.textToTokenList(text);
	}
}
