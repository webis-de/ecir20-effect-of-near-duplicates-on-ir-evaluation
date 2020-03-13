package de.webis.trec_ndd.trec_collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

public class QrelParsingTest {
	@Test
	public void approveJudgedIdsInClueweb09() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.CLUEWEB09);
		
		Assert.assertEquals(72699, judgedIds.size());
		Assert.assertEquals("clueweb09-en0000-00-00142", judgedIds.get(0));
		Assert.assertEquals("clueweb09-enwp03-59-01036", judgedIds.get(judgedIds.size()-1));
		Assert.assertTrue(judgedIds.contains("clueweb09-en0003-55-31884"));
		Assert.assertTrue(judgedIds.contains("clueweb09-en0007-05-20194"));
	}
	
	@Test
	public void approveJudgedIdsInClueweb12() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.CLUEWEB12);
		
		Assert.assertEquals(28746, judgedIds.size());
		Assert.assertEquals("clueweb12-0000tw-00-02137", judgedIds.get(0));
		Assert.assertEquals("clueweb12-1914wb-27-21351", judgedIds.get(judgedIds.size()-1));
	}
	
	@Test
	public void approveJudgedIdsInGov2() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.GOV2);
		
		Assert.assertEquals(128010, judgedIds.size());
		Assert.assertEquals("GX000-00-0000000", judgedIds.get(0));
		Assert.assertEquals("GX272-84-8825086", judgedIds.get(judgedIds.size()-1));
	}
	
	@Test
	public void approveJudgedIdsInCommonCore2018() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.CORE2018);
		
		Assert.assertEquals(24642, judgedIds.size());
		Assert.assertEquals("0001e59ffee8d5137eec54c92dc22bad", judgedIds.get(0));
		Assert.assertEquals("fffea646-f90f-11e1-a93b-7185e3f88849", judgedIds.get(judgedIds.size()-1));
	}
	
	@Test
	public void approveJudgedIdsInCommonCore2017() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.CORE2017);
		
		Assert.assertEquals(29080, judgedIds.size());
		Assert.assertEquals("0758170", judgedIds.get(0));
		Assert.assertEquals("999905", judgedIds.get(judgedIds.size()-1));
	}
	
	@Test
	public void approveJudgedIdsInMillionQuery() {
		List<String> judgedIds = judgedDocumentIdsInConfig(TrecCollections.GOV2_MQ);

		Assert.assertEquals(81802, judgedIds.size());
		Assert.assertEquals("GX000-00-0000000", judgedIds.get(0));
		Assert.assertEquals("GX272-84-8558203", judgedIds.get(judgedIds.size()-1));
	}
	
	private static List<String> judgedDocumentIdsInConfig(CollectionConfiguration config) {
		List<String> ret = new ArrayList<>(config.judgedDocumentIds());
		Collections.sort(ret);
		
		return ret;
	}
}
