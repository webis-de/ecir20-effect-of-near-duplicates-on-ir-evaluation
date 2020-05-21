package de.webis.trec_ndd.trec_collections;

import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.codehaus.jackson.map.ObjectMapper;

import de.webis.trec_ndd.util.TextCanonicalization;
import io.anserini.index.generator.LuceneDocumentGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("serial")
public class CollectionDocument implements Serializable {
	private String id,
				   content,
				   fullyCanonicalizedContent;
	
	private URL url,
				canonicalUrl;
	
	public static CollectionDocument fromLuceneDocument(Document document) {
		String content = document.get(LuceneDocumentGenerator.FIELD_BODY);
		String documentId = AnseriniCollectionReader.documentId(document);

		return collectionDocument(content, documentId);
	}
	
	public static CollectionDocument collectionDocument(String content, String id) {
		List<String> canonicalizedTokens = TextCanonicalization.fullCanonicalization(content); 
		
		return new CollectionDocument(
			id,
			content,
			canonicalizedTokens.stream().collect(Collectors.joining(" ")),
			null,
			null
		);
	}
	
	@Override
	@SneakyThrows
	public String toString() {
		return new ObjectMapper().writeValueAsString(this);
	}
	
	@SneakyThrows
	public static CollectionDocument fromString(String str) {
		return new ObjectMapper().readValue(str, CollectionDocument.class);
	}
}
