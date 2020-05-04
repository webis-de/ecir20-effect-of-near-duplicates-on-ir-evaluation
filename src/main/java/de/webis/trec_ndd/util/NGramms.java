package de.webis.trec_ndd.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import de.webis.trec_ndd.similarity.MD5;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class NGramms {

	private static final Pattern WHITESPACE = Pattern.compile("\\s+");
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class Word8Gramm implements Serializable {
		private String nGramm;
		private String md5Hash;
	}
	
	public static List<Word8Gramm> build8Gramms(String text) {
		return build8Gramms(tokenize(text));
	}
	
	public static List<String> nGramms(String text, int length) {
		return nGramms(tokenize(text), length);
	}
	
	public static List<String> tokenize(String text) {
		return Arrays.asList(WHITESPACE.split(text));
	}
	
	public static List<String> nGramms(List<String> tokens, int length) {
		List<String> ret = new LinkedList<>();
		
		for(int i=0; i<= tokens.size() - length; i++) {
			ret.add(buildN8GramStartingAt(i, tokens, length));
		}
		
		return ret;
	}
	
	public static List<Word8Gramm> build8Gramms(List<String> tokens) {
		return nGramms(tokens, 8).stream()
				.map(i -> new Word8Gramm(i, MD5.md5hash(i)))
				.collect(Collectors.toList());
	}
	
	private static String buildN8GramStartingAt(int position, List<String> tokens, int length) {
		String ret = "";
		
		for(int i = 0; i< length; i++) {
			ret += tokens.get(position + i) + " ";
		}
		
		return ret.trim();
	}
}
