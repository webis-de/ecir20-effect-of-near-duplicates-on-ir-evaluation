package de.webis.trec_ndd.trec_eval;

import org.approvaltests.Approvals;
import org.junit.Ignore;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class EvaluationOnClueWeb12Test {
	
	@Test
	public void approveBestERR_10RunFromTable2From2013() {
		// clustmrfaf 0.175
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2013, "clustmrfaf"));
	}
	
	@Test
	public void approveWorstMapRunFromTable2From2013() {
		// dlde 0.008
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2013, "dlde"));
	}
	
	@Test
	public void approveuogTrAIwLmbFromTable2From2013() {
		// uogTrAIwLmb 0.151
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2013, "uogTrAIwLmb"));
	}
	
	@Test
	public void approveXXXRunFromTable2From2013() {
		// udemQlm1lFbWiki
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2013, "udemQlm1lFbWiki"));
	}
	
	@Test
	@Ignore
	public void approveBestERR_IA_20RunFromTable2From2014() {
		// UDInfoWebLES 0.688
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2014, "UDInfoWebLES"));
	}
	
	@Test
	public void approveWorstERR_IA_20RunFromTable2From2014() {
		// SNUMedinfo12 0.531
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2014, "SNUMedinfo12"));
	}
	
	@Test
	public void approveTerranFromTable2From2014() {
		// Terran 0.578
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2014, "Terran"));
	}
	
	@Test
	public void approveudelwebisWt14axMaxRunFromTable2From2014() {
		// webisWt14axMax 0.589
		Approvals.verifyAsJson(EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2014, "webisWt14axMax"));
	}
}
