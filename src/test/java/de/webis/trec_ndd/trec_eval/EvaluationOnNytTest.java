package de.webis.trec_ndd.trec_eval;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

import static de.webis.trec_ndd.trec_eval.EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun;

public class EvaluationOnNytTest {
	@Test
	public void approveBestMapRunFromTable2() {
		// UWatMDS_TARSv1 0.462
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2017, "UWatMDS_TARSv1"));
	}
	
	@Test
	public void approveWorstMapRunFromTable2() {
		// ims_wcs_ap_uf 0.019
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2017, "ims_wcs_ap_uf"));
	}
	
	@Test
	public void approvetgncorpBOOSTFromTable2() {
		// tgncorpBOOST 0.276
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2017, "tgncorpBOOST"));
	}
	
	@Test
	public void approvetgncorpwebis_baseline2FromTable2() {
		// webis_baseline2 0.066
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.CORE_2017, "webis_baseline2"));
	}
}
