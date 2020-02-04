package de.webis.trec_ndd.trec_collections;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import lombok.Data;

public class Bla {
	@Data
	public static class BLA {
		private final String topicNumber;
		private final String topicQuery;
		private final Map<String, String> documentToJudgment; 
	}
	
	public static void main(String [] args) throws Exception {
		Map<String, Object> ret = new LinkedHashMap<>();
		
		for(SharedTask task : TrecCollections.CLUEWEB09.getSharedTasks()) {
			ret.put(task.toString(), taskStuff(task));
		}
		
		new ObjectMapper().writeValue(new File("clueweb.json"), ret);
	}
	
	private static Map<String, BLA> taskStuff(SharedTask task) {
		Map<String, BLA> ret = new HashMap<>();
		
		for(Map.Entry<String, Map<String, String>> i : task.documentJudgments().getData().entrySet()) {
			String query = task.getQueryForTopic(i.getKey());
			ret.put(i.getKey(), new BLA(i.getKey(), query, i.getValue()));
		}
		
		return ret;
	}
}
