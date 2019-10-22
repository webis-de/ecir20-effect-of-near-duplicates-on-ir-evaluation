package de.webis.trec_ndd.trec_eval;

import static de.webis.trec_ndd.trec_eval.EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class EvaluationOnWaPoTest {
	@Test
	public void approveBestMapRunFromTable1() {
		//-c -q -M1000
		
		
		// RMITUQVDBFNZDM1 0.385
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2018, "RMITUQVDBFNZDM1"));
	}
	
	@Test
	public void approveWorstMapRunFromTable1() {
		// feup-run4 0.003
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2018, "feup-run4"));
	}
	
	@Test
	public void approvetgncorpsabopt50v45FromTable1() {
		// sabopt50v45 0.227
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2018, "sabopt50v45"));
	}
	
	@Test
	public void approvetgncorpwebis_baseline2FromTable1() {
		// anserini_ax17 0.206
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2018, "anserini_ax17"));
	}
}
