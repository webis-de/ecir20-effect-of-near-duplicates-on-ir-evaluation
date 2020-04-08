package de.webis.trec_ndd.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

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
	
	public static List<String> tokenize(String text) {
		return Arrays.asList(WHITESPACE.split(text));
	}
	
	public static List<Word8Gramm> build8Gramms(List<String> tokens) {
		int nGrammLength = 8;
		List<Word8Gramm> ret = new LinkedList<>();
		
		for(int i=0; i<= tokens.size() - nGrammLength; i++) {
			ret.add(buildWord8GramStartingAt(i, tokens));
		}
		
		return ret;
	}
	
	private static Word8Gramm buildWord8GramStartingAt(int position, List<String> tokens) {
		String ret = "";
		
		for(int i = 0; i< 8; i++) {
			ret += tokens.get(position + i) + " ";
		}
		
		ret = ret.trim();
		
		return new Word8Gramm(ret, MD5.md5hash(ret));
	}
}
