package de.webis.trec_ndd.similarity;

import java.util.Arrays;

import org.apache.solr.update.processor.TextProfileSignature;
import org.apache.solr.client.solrj.SolrQuery;

public class TextProfileSignatureSimilarity implements TextSimilarity {

	@Override
	public boolean textsAreSimilar(String a, String b) {
		return Arrays.equals(textProfileSignature(a), textProfileSignature(b));
	}

	public static byte[] textProfileSignature (String text) {
		TextProfileSignature signature = new TextProfileSignature();
		signature.init(new SolrQuery());
		signature.add(text);
		return signature.getSignature();
	}
	
	public static String textProfileSignatureString (String text) {
		StringBuilder sb=new StringBuilder();
		for (byte a : textProfileSignature (text)) sb.append(String.format("%02X",a));
		return sb.toString().toLowerCase();
	}
}
