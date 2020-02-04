package de.webis.trec_ndd.spark;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import scala.Tuple2;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@SuppressWarnings("serial")
public class DocumentGroup implements Serializable {
	
	public String hash;
	public ArrayList<String> ids = new ArrayList<String>();
	
	public DocumentGroup (String line) {
		DocumentGroup parsedGroup = createFromJson(line);
		this.hash = parsedGroup.hash;
		this.ids = parsedGroup.ids;
	}
	
	public DocumentGroup (Tuple2<String, Iterable<Tuple2<String, String>>> group) {
		this.hash=group._1;
		group._2.forEach(tuple -> this.ids.add(tuple._2));
	}
	
	@SneakyThrows
	public String toString() {
		return new ObjectMapper().writeValueAsString(this);
	}
	
	@SneakyThrows
	public static List<DocumentGroup> readFromJonLines(Path path) {
		return Files.readAllLines(path).stream()
				.map(s -> createFromJson(s))
				.collect(Collectors.toList());
	}
	
	@SneakyThrows
	public static DocumentGroup createFromJson(String jsonLine) {
		return new ObjectMapper().readValue(jsonLine, DocumentGroup.class);
	}

	public boolean isUsed(HashSet<String> usedDocIDs) {
		for(String id : ids) if(usedDocIDs.contains(id)) return true;
		return false;
	}
	
	public DocumentGroup copy() {
		return new DocumentGroup(this.getHash(), new ArrayList<>(this.getIds()));
	}

	public static List<DocumentGroup> appendRetrievalEquivalentToContentEquivalent(List<DocumentGroup> contentEquivalent, List<DocumentGroup> retrievalEquivalent) {
		List<DocumentGroup> ret = new LinkedList<>(contentEquivalent);
		
		for(DocumentGroup retEq: retrievalEquivalent) {
			boolean shouldAddCompleteRetEq = Boolean.TRUE;
			
			for(DocumentGroup contEq: contentEquivalent) {
				for(String id: retEq.getIds()) {
					if(contEq.getIds().contains(id)) {
						contEq.getIds().addAll(retEq.getIds());
						shouldAddCompleteRetEq = Boolean.FALSE;
						continue;
					}
				}
			}
			
			if(shouldAddCompleteRetEq) {
				ret.add(retEq);
			}
		}
		
		for(DocumentGroup docGroup : ret) {
			docGroup.setIds(new ArrayList<>(new TreeSet<>(docGroup.getIds())));
		}
		
		return ret;
	}
}



