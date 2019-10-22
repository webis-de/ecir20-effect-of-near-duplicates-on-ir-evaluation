package de.webis.trec_ndd.spark;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import de.webis.trec_ndd.similarity.MD5;
import de.webis.trec_ndd.similarity.TextProfileSignatureSimilarity;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@Accessors(chain = true)
@SuppressWarnings("serial")
public class DocumentHash implements Serializable {
	private String id;
	private String textProfileSignature;
	private String md5;
	private String fullyTextProfileSignature;
	private String fullyCanonicalizedMd5;
	private long documentLength;
	private long fullyCanonicalizedWord8GrammCount;
	private long fullyCanonicalizedWord8GrammSetSize;
	
	public DocumentHash (CollectionDocument doc) {
		this.id=doc.getId();
		this.md5=MD5.md5hash(doc.getContent());
		this.textProfileSignature=TextProfileSignatureSimilarity.textProfileSignatureString(doc.getContent());
		this.documentLength=doc.getContent().length();
		this.fullyTextProfileSignature = TextProfileSignatureSimilarity.textProfileSignatureString(doc.getFullyCanonicalizedContent());
		this.fullyCanonicalizedMd5 = MD5.md5hash(doc.getFullyCanonicalizedContent());
		List<Word8Gramm> word8Gramms = NGramms.build8Gramms(doc.getFullyCanonicalizedContent());
		
		this.fullyCanonicalizedWord8GrammCount = word8Gramms.size();
		this.fullyCanonicalizedWord8GrammSetSize = new HashSet<>(word8Gramms).size();
	}
	
	@SneakyThrows
	public String toString() {
		return new ObjectMapper().writeValueAsString(this);
	}
	
	@SneakyThrows
	public static DocumentHash fromString(String str) {
		return new ObjectMapper().readValue(str, DocumentHash.class);
	}
}
