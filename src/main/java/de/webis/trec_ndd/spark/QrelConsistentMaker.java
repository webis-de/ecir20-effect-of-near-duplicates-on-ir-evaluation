package de.webis.trec_ndd.spark;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.collections.list.UnmodifiableList;

import de.webis.trec_ndd.spark.RunResultDeduplicator.DeduplicationResult;
import de.webis.trec_ndd.trec_collections.Qrel;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import lombok.SneakyThrows;
import scala.Tuple2;

public interface QrelConsistentMaker {
	
	public static final QrelConsistentMaker 
		base=(judgements, groups, deduplication) -> Internals.changeQrel(Internals::base, judgements,  groups),
		maxValue=(judgements, groups, deduplication) -> Internals.changeQrel(Internals::maxValue, judgements,  groups),
		maxValueDuplicateDocsIrrelevant=(judgements, groups, deduplication) -> Internals.applyDeduplication(Internals.changeQrel(Internals::maxValue, judgements,  groups), deduplication),
		maxValueAllDuplicateDocsIrrelevant=(judgements, groups, deduplication) -> Internals.makeMultipleRelevantDocumentsPerGroupIrrelevant(Internals.applyDeduplication(Internals.changeQrel(Internals::maxValue, judgements,  groups), deduplication), groups)
//		minValue=(judgements, groups) -> Internals.changeQrel(Internals::minValue, judgements,  groups),
//		avgValue=(judgements, groups) -> Internals.changeQrel(Internals::avgValue, judgements,  groups)
	;
	
	public static final List<QrelConsistentMaker> all = Internals.getInstances();
	
	public HashSet<QrelEqualWithoutScore> getQrels(Set<QrelEqualWithoutScore> judgements, List<DocumentGroup> groups, DeduplicationResult deduplication);
	
	public default String name() {
		return Internals.names.get(this);
	}

	public static class Internals{
		
		public static Map<QrelConsistentMaker, String> names = initNames();
		
		@SneakyThrows
		public static Map<QrelConsistentMaker, String> initNames(){
			return Arrays.asList(QrelConsistentMaker.class.getDeclaredFields())
				.stream()
				.filter(Internals::isInstanceOfThisClass)
				.collect(Collectors.toMap(Internals::convertField , f-> f.getName()));
		}
		
		public static HashSet<QrelEqualWithoutScore> makeMultipleRelevantDocumentsPerGroupIrrelevant(HashSet<QrelEqualWithoutScore> qrels, List<DocumentGroup> groups) {
			HashSet<QrelEqualWithoutScore> ret = new HashSet<>();
			Map<Integer, List<QrelEqualWithoutScore>> qrelsPerTopic = copyAndGroupQrelsByTopic(qrels);
			Map<String, Set<String>> documentToEquivalentDocuments = buildDocumentToEquivalentDocumentsMap(groups);
			
			for(List<QrelEqualWithoutScore> topic: qrelsPerTopic.values()) {
				Set<String> idsToMarkIrrelevant = new HashSet<>();
				
				for(QrelEqualWithoutScore qrel : topic) {
					// we can assume that maxValueDuplicateDocsIrrelevant has already marked documents not in the ranking as irrelevant
					if (qrel.getScore() > 0) {
						if (idsToMarkIrrelevant.contains(qrel.getDocumentID())){
							qrel.setScore(0);
						} else if(documentToEquivalentDocuments.containsKey(qrel.getDocumentID())) {
							idsToMarkIrrelevant.addAll(documentToEquivalentDocuments.get(qrel.getDocumentID()));
						}
					}
					
					ret.add(qrel);
				}
			}
			
			return ret;
		}

		private static Map<String, Set<String>> buildDocumentToEquivalentDocumentsMap(List<DocumentGroup> groups) {
			Map<String, Set<String>> ret = new HashMap<>();
			
			for(DocumentGroup group : groups) {
				Set<String> ids = new HashSet<>(group.ids);
				
				for(String id: ids) {
					if(ret.containsKey(id)) {
						throw new RuntimeException("This should not happen.");
					}
					
					ret.put(id, ids);
				}
			}
			
			return ret;
		}

		private static Map<Integer, List<QrelEqualWithoutScore>> copyAndGroupQrelsByTopic(Set<QrelEqualWithoutScore> qrels) {
			Map<Integer, List<QrelEqualWithoutScore>> ret = new LinkedHashMap<>();
			
			for(QrelEqualWithoutScore qrel : qrels) {
				if(!ret.containsKey(qrel.getTopicNumber())) {
					ret.put(qrel.getTopicNumber(), new LinkedList<>());
				}
				
				ret.get(qrel.getTopicNumber()).add(qrel.copy());
			}
			
			for(List<QrelEqualWithoutScore> topic :ret.values()) {
				Collections.sort(topic, (a,b) -> a.getDocumentID().compareTo(b.getDocumentID()));
			}
			
			return ret;
		}
		
		public static HashSet<QrelEqualWithoutScore> applyDeduplication(HashSet<QrelEqualWithoutScore> qrels, DeduplicationResult deduplication) {
			qrels = new HashSet<>(qrels.stream().map(QrelEqualWithoutScore::copy).collect(Collectors.toSet()));
			Iterator<QrelEqualWithoutScore> iter = qrels.iterator();
			
			while(iter.hasNext()) {
				QrelEqualWithoutScore next = iter.next();
				
				Set<String> idsToRemove = deduplication.getTopicToDocumentIdsToMakeIrrelevant().get(next.getTopicNumber());
				
				if(idsToRemove != null && idsToRemove.contains(next.getDocumentID())) {
					next.setScore(0);
				}
			}
			
			return qrels;
		}

		@SuppressWarnings("unchecked")
		public static List<QrelConsistentMaker> getInstances() {
			return UnmodifiableList.decorate(Arrays.asList(QrelConsistentMaker.class.getDeclaredFields()).stream()
					.filter(Internals::isInstanceOfThisClass)
					.map(Internals::convertField)
					.collect(Collectors.toList()));
		}
		
		@SneakyThrows
		static boolean isInstanceOfThisClass(Field f) {
			return f.get(QrelConsistentMaker.class) instanceof QrelConsistentMaker;
		}
		
		@SneakyThrows
		static QrelConsistentMaker convertField(Field f) {
			return (QrelConsistentMaker) f.get(QrelConsistentMaker.class);
		}
		
		@SneakyThrows
		public static HashSet<QrelEqualWithoutScore> changeQrel(BiFunction<DocumentGroup,HashMap<String,ArrayList<Qrel>>,HashMap<Integer, Integer>> func, Set<QrelEqualWithoutScore> judgements, List<DocumentGroup> groups) {
			HashMap<String,ArrayList<Qrel>> idMapQrels=createMapById(judgements);
			HashSet<QrelEqualWithoutScore> output=new HashSet<QrelEqualWithoutScore>();
			output.addAll(judgements);
			QrelEqualWithoutScore qrel;
			for(DocumentGroup currentGroup: groups) {
				HashMap<Integer, Integer> topicValueMap = func.apply(currentGroup, idMapQrels);
				for(Integer topic: topicValueMap.keySet()) 
					for(String id: currentGroup.ids) {
						qrel = new QrelEqualWithoutScore();
						qrel.setDocumentID(id).setTopicNumber(topic).setScore(topicValueMap.get(topic));
						if(output.contains(qrel)) output.remove(qrel); 
						output.add(qrel);
					}
			}
			return output;
		}
		
		
		@SneakyThrows
		static HashMap<String,ArrayList<Qrel>> createMapById(Collection<? extends Qrel> judgements) {
			HashMap<String,ArrayList<Qrel>> map = new HashMap<String,ArrayList<Qrel>>();
			for(Qrel judgement: judgements) {
				if(!map.containsKey(judgement.getDocumentID())) map.put(judgement.getDocumentID(), new ArrayList<Qrel>());
				map.get(judgement.getDocumentID()).add(judgement);
			}
			return map;
		}

		
		@SneakyThrows
		static HashMap<Integer, Integer> extremeValue(DocumentGroup group, HashMap<String,ArrayList<Qrel>> idMapQrel, BiFunction<Integer,Qrel,Boolean> func){
			HashMap<Integer, Integer> topicValueMap = new HashMap<Integer,Integer>();
			for(String id: group.ids) {
				if(idMapQrel==null || id==null) throw new Exception("idMapQrel is null = "+(idMapQrel==null)+" // id is null = "+(id==null));
				if(!idMapQrel.containsKey(id)) continue;
				for(Qrel qrel: idMapQrel.get(id)) {
					if(!topicValueMap.containsKey(qrel.getTopicNumber()) || func.apply(topicValueMap.get(qrel.getTopicNumber()),qrel) )
						topicValueMap.put(qrel.getTopicNumber(), qrel.getScore());
				}
			}
			return topicValueMap;
		}
		
		@SneakyThrows
		static HashMap<Integer, Integer> avgValue(DocumentGroup group, HashMap<String,ArrayList<Qrel>> idMapQrel){
			HashMap<Integer, Integer> topicValueMap = new HashMap<Integer,Integer>();
			HashMap<Integer, Tuple2<Integer,Integer>> topicValueTupleMap = new HashMap<Integer,Tuple2<Integer,Integer>>();
			Tuple2<Integer,Integer> temp;
			for(String id: group.ids) {
				if(idMapQrel==null || id==null) throw new Exception("idMapQrel is null = "+(idMapQrel==null)+" // id is null = "+(id==null));
				if(!idMapQrel.containsKey(id)) continue;
				for(Qrel qrel: idMapQrel.get(id)) {
					if(!topicValueTupleMap.containsKey(qrel.getTopicNumber()) ) topicValueTupleMap.put(qrel.getTopicNumber(), new Tuple2<Integer,Integer>(0,0));
					temp = topicValueTupleMap.get(qrel.getTopicNumber());
					temp = new Tuple2<Integer,Integer>(temp._1+1, temp._2+qrel.getScore());
					topicValueTupleMap.put(qrel.getTopicNumber(),temp);
				}
			}
			for(Integer key : topicValueTupleMap.keySet()) {
				temp=topicValueTupleMap.get(key);
				topicValueMap.put(key, (int) Math.round(temp._2/(double)temp._1));
			}
			return topicValueMap;
		}
		
		@SneakyThrows
		static HashMap<Integer, Integer> maxValue(DocumentGroup group, HashMap<String,ArrayList<Qrel>> idMapQrel){
			return extremeValue(group, idMapQrel, (value,qrel)-> value<qrel.getScore());
		}
		
		@SneakyThrows
		static HashMap<Integer, Integer> minValue(DocumentGroup group, HashMap<String,ArrayList<Qrel>> idMapQrel){
			return extremeValue(group, idMapQrel, (value,qrel)-> value>qrel.getScore());
		}
		
		static HashMap<Integer, Integer> base(DocumentGroup group, HashMap<String,ArrayList<Qrel>> idMapQrel){
			return new HashMap<Integer,Integer>();
		}
	}
}
