package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.Qrel;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.util.Reader;
import lombok.SneakyThrows;

public class QrelConsistentMakerTest {

	
	private DocumentGroup group;
	private HashMap<String,ArrayList<Qrel>> idMapQrel;
	private static String line = "{\"hash\":\"ac2d11c5d7f77cdef38f1566f3df8b05\",\"ids\":[\"clueweb09-ja0004-63-23900\",\"clueweb09-ja0009-69-49575\",\"clueweb09-ja0014-01-20176\"]}";
	
	public QrelConsistentMakerTest(){
		group =  new DocumentGroup(line);
		idMapQrel = new HashMap<String,ArrayList<Qrel>>();
		idMapQrel.put(group.ids.get(0),new ArrayList<Qrel>());
		idMapQrel.get(group.ids.get(0)).add(new Qrel().setDocumentID(group.ids.get(0)).setTopicNumber(1).setScore(4));
		idMapQrel.get(group.ids.get(0)).add(new Qrel().setDocumentID(group.ids.get(0)).setTopicNumber(2).setScore(10));
		idMapQrel.put(group.ids.get(1),new ArrayList<Qrel>());
		idMapQrel.get(group.ids.get(1)).add(new Qrel().setDocumentID(group.ids.get(1)).setTopicNumber(11).setScore(6));
		idMapQrel.get(group.ids.get(1)).add(new Qrel().setDocumentID(group.ids.get(1)).setTopicNumber(1).setScore(8));
		
	}
	
	@Test
	public void testMaxValue() {
		QrelConsistentMakerTest instance = new QrelConsistentMakerTest();
		HashMap<Integer,Integer> topicMapValue = QrelConsistentMaker.Internals.maxValue(instance.group,instance.idMapQrel);
		Approvals.verifyAsJson(topicMapValue);
	}
	
	@Test
	public void testMinValue() {
		QrelConsistentMakerTest instance = new QrelConsistentMakerTest();
		HashMap<Integer,Integer> topicMapValue = QrelConsistentMaker.Internals.minValue(instance.group,instance.idMapQrel);
		Approvals.verifyAsJson(topicMapValue);
	}
	
	@Test
	public void testAvgValue() {
		QrelConsistentMakerTest instance = new QrelConsistentMakerTest();
		HashMap<Integer,Integer> topicMapValue = QrelConsistentMaker.Internals.avgValue(instance.group,instance.idMapQrel);
		Approvals.verifyAsJson(topicMapValue);
	}
	
	@Test
	public void testCreateMapById() {
		ArrayList<Qrel> qrelList = new ArrayList<Qrel>();
		qrelList.add(new Qrel().setDocumentID("doc1").setTopicNumber(1).setScore(10));
		qrelList.add(new Qrel().setDocumentID("doc1").setTopicNumber(2).setScore(2));
		qrelList.add(new Qrel().setDocumentID("doc1").setTopicNumber(3).setScore(3));
		qrelList.add(new Qrel().setDocumentID("doc2").setTopicNumber(1).setScore(6));
		HashMap<String, ArrayList<Qrel>> idMapQrelTest = QrelConsistentMaker.Internals.createMapById(qrelList);
		Approvals.verifyAsJson(idMapQrelTest);
	}
	

	
	//TODO 
	//test all configs
	@Test
	@SneakyThrows
	public void testWriteChangedQrel() {
		HashSet<QrelEqualWithoutScore> judgements = new HashSet<QrelEqualWithoutScore>();
		judgements.addAll(new Reader<QrelEqualWithoutScore>("./src/test/resources/QrelConsistentMaker/qrelFile", QrelEqualWithoutScore.class).getCollection());
		List<DocumentGroup> groups = new Reader<DocumentGroup>("./src/test/resources/QrelConsistentMaker/groupFile", DocumentGroup.class).getArrayList();
		Approvals.verifyAsJson(QrelConsistentMaker.base.getQrels(judgements,groups, null));
	}
	
	
}
