package de.webis.trec_ndd.trec_collections;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.junit.Test;

import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class TrecSharedTaskTest {
	
	public static Set<String> getQrelIdSet(SharedTask sharedTask){
		Set<String> qrelSet = new HashSet<String>();
		Scanner sc = new Scanner(sharedTask.getQrelResourceAsStream());
		String line; Qrel q;
		while(sc.hasNextLine()) {
			line = sc.nextLine();
			q = new Qrel(line);
			qrelSet.add(q.getDocumentID());
		}
		sc.close();
		return qrelSet;
	}
	public static List<String> getIdsInGroupsAsList(SharedTask sharedTask) {
		Scanner sc = new Scanner(sharedTask.getGroupFingerprintResourceAsStream());
		List<String> idsInGroups = new ArrayList<String>();
		String line; DocumentGroup dg;
		while(sc.hasNextLine()) {
			line = sc.nextLine();
			dg = new DocumentGroup(line);
			idsInGroups.addAll(dg.getIds());
		}
		sc.close();
		return idsInGroups;
	}
	@Test
	public void groupsAreContainedInQrel() throws Exception {
		//setup
		for(SharedTask sharedTask: TrecSharedTask.values())
		{
			if(sharedTask.getGroupFingerprintResource()==null)
				continue; //no groups given
			Set<String> qrelSet = getQrelIdSet(sharedTask);
			long groupCount = getIdsInGroupsAsList(sharedTask).stream()
				.filter(id -> qrelSet.contains(id)).count();
			if(groupCount<8) throw new Exception("unlikely low number of documents in groups are contained in qrels");
		}
	}
}
