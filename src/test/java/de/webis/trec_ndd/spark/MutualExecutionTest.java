package de.webis.trec_ndd.spark;

import java.util.HashMap;
import java.util.Map;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.util.MutualExecution;

public class MutualExecutionTest {
	@Test
	public void approveMutualExecutionForAllQrelStrategiesAndAllDeduplications() {
		Map<String, Map<String, Boolean>> executions = new HashMap<>();
		
		for(QrelConsistentMaker qrel: QrelConsistentMaker.all) {
			Map<String, Boolean> execForQrel = new HashMap<>();
			
			for(RunResultDeduplicator dedup: RunResultDeduplicator.all(null)) {
				//really ugly hack to control the execution in the current framework
				execForQrel.put(dedup.name(), MutualExecution.shouldExecuteTogether(qrel, dedup));
			}
			
			executions.put(qrel.name(), execForQrel);
		}
		
		Approvals.verifyAsJson(executions);
	}
}
