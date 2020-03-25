package de.webis.trec_ndd.trec_collections;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.trec_eval.EvaluationMeasure;
import io.anserini.search.topicreader.TopicReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import io.anserini.search.topicreader.WebxmlTopicReader;
import io.anserini.search.topicreader.TrecTopicReader;

public interface SharedTask {
	String getQrelResource();
	public List<EvaluationMeasure> getOfficialEvaluationMeasures();
	public List<EvaluationMeasure> getInofficialEvaluationMeasures();
	public List<String> runFiles();
	public String name();
	public Map<String, Map<String, String>> topicNumberToTopic(); 
	
	public default InputStream getQrelResourceAsStream() {
		return SharedTask.class.getResourceAsStream(getQrelResource());
	}
	
	public default List<EvaluationMeasure> getEvaluationMeasures() {
		Set<EvaluationMeasure> ret = new HashSet<>(getOfficialEvaluationMeasures());
		ret.addAll(getInofficialEvaluationMeasures());
		
		return ret.stream().collect(Collectors.toList());
	}
	
	@SneakyThrows
	public default Stream<Pair<String, List<RunLine>>> rankingResults() {
		return runFiles().stream()
				.map(f -> Pair.of(f, RunLine.parseRunlines(Paths.get(f))));
	}
	
	public default Set<String> documentIdsInRunFiles() {
		Set<String> ret = new HashSet<>();
		
		rankingResults().forEach(result -> {
			result.getRight().stream()
			.map(RunLine::getDoucmentID)
			.forEach(ret::add);
		});
		
		return ret;
	}
	
	public default String getQueryForTopic(String topicNumber) {
		Map<String, Map<String, String>> bla = topicNumberToTopic();
		return bla.get(topicNumber).get("title");
	}
	
	public default Set<QrelEqualWithoutScore> getQrelResourcesWithoutScore() {
		BufferedReader reader = new BufferedReader(new InputStreamReader(getQrelResourceAsStream()));
		
		return Collections.unmodifiableSet(new HashSet<>(reader.lines()
				.map(line -> new QrelEqualWithoutScore(line))
				.collect(Collectors.toCollection(ArrayList::new))));
	}
	
	public default DocumentJudgments documentJudgments() {
		//Topic<DocID,Judgement>
		Map<String, Map<String, String>> data = new HashMap<>();
		Map<String, List<String>> irrelevant = new HashMap<>();
		Map<String, List<String>> relevant = new HashMap<>();
		
		for(QrelEqualWithoutScore qrel : getQrelResourcesWithoutScore()) {
			String topic = String.valueOf(qrel.getTopicNumber());
			if(!data.containsKey(topic)) {
				data.put(topic, new HashMap<>());
			}
			data.get(topic).put(qrel.getDocumentID(), String.valueOf(qrel.getScore()));
			if(qrel.getScore()<=0) {
				if(!irrelevant.containsKey(topic)) {
					irrelevant.put(topic, new ArrayList<String>());
				}
				irrelevant.get(topic).add(qrel.getDocumentID());
			}
			else {
				if(!relevant.containsKey(topic)) {
					relevant.put(topic, new ArrayList<String>());
				}
				relevant.get(topic).add(qrel.getDocumentID());
			}
		}
		
		return new DocumentJudgments(data, irrelevant, relevant);
	}
	
	@Data
	public static class DocumentJudgments {
		private static final String UNKNOWN_LABEL = "UNKNOWN";
		
		private final Map<String, Map<String, String>> data; //Topic, <ID, Judgement>
		private final Map<String, List<String>> irrelevant; //Topic, List<IDs>
		private final Map<String, List<String>> relevant;

		public List<String> topics() {
			return new ArrayList<>(data.keySet());
		}

		public String labelForTopicAndDocument(String topic, String document) {
			return data.get(topic).getOrDefault(document, UNKNOWN_LABEL);
		}
		
		public Set<String> labelsInGroupForTopic(String topic, DocumentGroup docGroup) {
			return docGroup.ids.stream()
					.map(document -> labelForTopicAndDocument(topic, document))
					.collect(Collectors.toSet());
		}

		public boolean groupHasInconsistency(DocumentGroup docGroup) {
			for(String topic: topics()) {
				if(labelsInGroupForTopic(topic, docGroup).size() > 1) {
					return Boolean.TRUE;
				}
			}
			
			return Boolean.FALSE;
		}
		


		public boolean groupHasInconsistencyWithoutUnlabeled(DocumentGroup docGroup) {
			for(String topic: topics()) {
				Set<String> labels = labelsInGroupForTopic(topic, docGroup);
				labels.remove(UNKNOWN_LABEL);
				if(labels.size() > 1) {
					return Boolean.TRUE;
				}
			}
			
			return Boolean.FALSE;
		}

		public int judgmentCount() {
			return data.entrySet().stream()
					.mapToInt(i -> i.getValue().size())
					.sum();
		}

		public int documentsToJudgeAgain(DocumentGroup docGroup) {
			int ret = 0;
			for(String topic: topics()) {
				if(labelsInGroupForTopic(topic, docGroup).size() > 1) {
					ret += docGroup.ids.size();
				}
			}
			
			return ret;
		}
		
		public List<String> getIrrelevantDocuments(String topic){
			List<String> ret = this.irrelevant.get(topic);
			if(ret == null) {
				return new LinkedList<>();
			}
			
			return new LinkedList<String>(ret);
		}
		
		public List<String> getRelevantDocuments(String topic){
			List<String> ret = this.relevant.get(topic);
			if(ret == null) {
				return new LinkedList<>();
			}
			
			return new LinkedList<String>(ret);
		}
	}
	
	public static void main(String[] args) {
		System.out.println(new LinkedList<String>(null));
	}
	
	@SneakyThrows
	public default Path getQrelResourceAsFile() {
		Path tmpDir = Files.createTempDirectory("tmp-qrel");
		tmpDir.toFile().deleteOnExit();
		Path ret = tmpDir.resolve("qrels");
		
		String ndevalResourceName = StringUtils.replace(getQrelResource(), ".txt", ".ndeval.txt");
		InputStream ndevalResource = SharedTask.class.getResourceAsStream(ndevalResourceName);
		if(ndevalResource != null) {
			Files.copy(ndevalResource,  tmpDir.resolve("qrels.ndeval"));
		}
		
		Files.copy(getQrelResourceAsStream(), ret);
		
		return ret;
	}
	
	public default String[] argsForTrecEval() {
		if(this.equals(TrecSharedTask.TERABYTE_2006)) {
			return new String[] {};
		}
		
		return new String[] {"-M", "1000"};
	}
	
	@Getter
	@AllArgsConstructor
	public static enum TrecSharedTask implements SharedTask {
		
		//Washington Post
		CORE_2018(
				"trec27/core/",
				"/topics-and-qrels/qrels.core18.txt",
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.P_10, EvaluationMeasure.NDCG),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				null,
				null
		),
		
		//New York Times
		CORE_2017(
				"trec26/core/",
				"/topics-and-qrels/qrels.core17.txt",
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG, EvaluationMeasure.P_10),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				null,
				null
		),
		
		//https://trec.nist.gov/data/terabyte04.html
		TERABYTE_2004(
				"trec13/terabyte/",
				"/topics-and-qrels/qrels.701-750.txt",
				Arrays.asList(EvaluationMeasure.MAP),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				null,
				null
		),
		
		//https://trec.nist.gov/data/terabyte05.html
		TERABYTE_2005_ADHOC(
				"trec14/terabyte.adhoc",
				"/topics-and-qrels/qrels.751-800.txt",
				Arrays.asList(EvaluationMeasure.BPREF, EvaluationMeasure.MAP, EvaluationMeasure.P_20),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				null,
				null
		),
		
		TERABYTE_2006(
				"/trec15/terabyte-adhoc",
				"/topics-and-qrels/qrels.801-850.txt",
				Arrays.asList(EvaluationMeasure.BPREF, EvaluationMeasure.MAP, EvaluationMeasure.P_20
						
						//FIXME IMPLEMENT THIS
						//EvaluationMeasure.INF_AP
				),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				null,
				null
		),
		
		MILLION_QUERY_2007(
				null,
				"/topics-and-qrels/prels.mq.1-10000.txt",
				Arrays.asList(
						//FIXME
				),
				Arrays.asList(
						//FIXME
				),
				null,
				null
		),
		
		MILLION_QUERY_2008(
				null,
				"/topics-and-qrels/prels.mq.10001-20000.txt",
				Arrays.asList(
						//FIXME
				),
				Arrays.asList(
						//FIXME
				),
				null,
				null
		),
		
		ROBUST_04(
				null,
				"/topics-and-qrels/qrels.robust2004.txt",
				null,
				null,
				() -> new TrecTopicReader(null),
				"/topics-and-qrels/topics.robust04.301-450.601-700.txt"
		),
		
		WEB_2009(
				"/trec18/web.adhoc",
				//FIXME: Should I use both?
//				"/topics-and-qrels/prels.web.1-50.txt",
				"/topics-and-qrels/qrels.inofficial.web.1-50.txt",
				Arrays.asList(
						// FIXME Implement those measures 
						//EvaluationMeasure.E_MAP, EvaluationMeasure.E_P_5, EvaluationMeasure.E_P_10, EvaluationMeasure.E_P_20, 
				
						// FIXME These are only surrogates
						EvaluationMeasure.MAP, EvaluationMeasure.BPREF, EvaluationMeasure.P_20
				),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.1-50.txt"
		),
		
		WEB_2010(
				"/trec19/web.adhoc",
				"/topics-and-qrels/qrels.web.51-100.txt",
				Arrays.asList(EvaluationMeasure.ERR_20, EvaluationMeasure.NDCG_CUT_20, EvaluationMeasure.P_20, EvaluationMeasure.MAP),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.51-100.txt"
		),
		
		WEB_2011(
				"/trec20/web.adhoc",
				"/topics-and-qrels/qrels.web.101-150.txt",
				Arrays.asList(EvaluationMeasure.ERR_20, EvaluationMeasure.NDCG_CUT_20, EvaluationMeasure.P_20, EvaluationMeasure.MAP),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.101-150.txt"
		),
		
		WEB_2012(
				"/trec21/web.adhoc",
				"/topics-and-qrels/qrels.web.151-200.txt",
				Arrays.asList(EvaluationMeasure.ERR_20, EvaluationMeasure.NDCG_CUT_20, EvaluationMeasure.P_20, EvaluationMeasure.MAP),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.151-200.txt"
		),

		WEB_2013(
				"/trec22/web.adhoc",
				"/topics-and-qrels/qrels.web.201-250.txt",
				Arrays.asList(EvaluationMeasure.ERR_10, EvaluationMeasure.NDCG_CUT_10, EvaluationMeasure.ERR_20,
						EvaluationMeasure.NDCG_CUT_20,
						EvaluationMeasure.ERR_IA_10, EvaluationMeasure.ALPHA_NDCG_CUT_10, EvaluationMeasure.NRBP,
						EvaluationMeasure.ERR_IA_20, EvaluationMeasure.ALPHA_NDCG_CUT_20
						
						//FIXME: They report that they evaluate MAP and P@20, but I cant find it?
						//EvaluationMeasure.P_20, EvaluationMeasure.MAP, 
				),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.201-250.txt"
		),
		
		WEB_2014(
				"/trec23/web.adhoc",
				"/topics-and-qrels/qrels.web.251-300.txt",
				Arrays.asList(EvaluationMeasure.ERR_IA_20, EvaluationMeasure.ALPHA_NDCG_CUT_20, EvaluationMeasure.NRBP, EvaluationMeasure.ERR_20, EvaluationMeasure.NDCG_CUT_20),
				Arrays.asList(EvaluationMeasure.MAP, EvaluationMeasure.NDCG),
				() -> new WebxmlTopicReader(null),
				"/topics-and-qrels/topics.web.251-300.txt"
		),

		SESSION_2010(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.session2010.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				null  // FIXME: Add topic resource
		),

		SESSION_2011(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.session2011.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				null  // FIXME: Add topic resource
		),
		
		SESSION_2012(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.session2012.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				null  // FIXME: Add topic resource
		),
		
		SESSION_2013(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.session2013.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				null  // FIXME: Add topic resource
		),
		
		SESSION_2014(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.session2014.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				null  // FIXME: Add topic resource
		),
		
		ENTITY_2009(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.inofficial.entity2009.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				"/topics-and-qrels/topics.entity2009.txt"
		),
		
		ENTITY_2010(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.inofficial.entity2010.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				"/topics-and-qrels/topics.entity2010.txt"
		),
		
		ENTITY_2011(
				null, // FIXME: Add runFileDirectory
				"/topics-and-qrels/qrels.entity2011.txt",
				null, //FIXME: Add officialEvaluationMeasures
				null, //FIXME: Add inofficialEvaluationMeasures,
				null, //FIXME: Add topicReader
				"/topics-and-qrels/topics.entity2011.txt"
		)
		;
		
		private final String runFileDirectory;
		private final String qrelResource;
		private final List<EvaluationMeasure> officialEvaluationMeasures;
		private final List<EvaluationMeasure> inofficialEvaluationMeasures;
		private final Supplier<TopicReader<?>> topicReader;
		private final String topicResource;
		
		@SneakyThrows
		public List<String> runFiles() {
			return Files.list(Paths.get("/mnt/nfs/webis20/data-in-progress/trec-system-runs/" + getRunFileDirectory()))
					.map(Object::toString)
					.collect(Collectors.toList());
		}

		@Override
		@SneakyThrows
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Map<String, Map<String, String>> topicNumberToTopic() {
			TopicReader<?> reader = topicReader.get();
			SortedMap<?, ?> tmp = reader.read(IOUtils.toString(TrecSharedTask.class.getResourceAsStream(topicResource), StandardCharsets.UTF_8));
			Map<String, Map<String, String>> ret = new HashMap<>();
			
			for(Map.Entry<?, ?> entry : tmp.entrySet()) {
				ret.put(String.valueOf(entry.getKey()), (Map) entry.getValue());
			}
			
			return ret;
		}
	}
}
