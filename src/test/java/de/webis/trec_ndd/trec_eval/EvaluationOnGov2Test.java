package de.webis.trec_ndd.trec_eval;

import static de.webis.trec_ndd.trec_eval.EvaluationOnClueWeb09Test.evaluateAllOfficialMeasuresForRun;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class EvaluationOnGov2Test {
	@Test
	public void approveBestMapRunFromTable1ForTerabyte2004() {
		// uogTBBaseL 0.305
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2004, "uogTBBaseL"));
	}
	
	@Test
	public void approveWorstMapRunFromTable1Terabyte2004() {
		// irttbtl 0.009
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2004, "irttbtl"));
	}
	
	@Test
	public void approvetgncorpsabopt50v45FromTable1Terabyte2004() {
		// MU04tb1 0.266
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2004, "MU04tb1"));
	}
	
	@Test
	public void approvetgncorpwebis_baseline2FromTable1Terabyte2004() {
		// THUIRtb2 0.056
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2004, "THUIRtb2"));
	}
	
	@Test
	public void approveBestMapRunFromTable1ForTerabyte2005() {
		// indri05AdmfS 0.4279
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2005_ADHOC, "indri05AdmfS"));
	}
	
	@Test
	public void approveWorstMapRunFromTable1Terabyte2005() {
		// uwmtEwtaD02t 0.2887
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2005_ADHOC, "uwmtEwtaD02t"));
	}
	
	@Test
	public void approvehumT05lFromTable1Terabyte2005() {
		// humT05l 0.3659
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2005_ADHOC, "humT05l"));
	}
	
	@Test
	public void approveNTUAH1FromTable1Terabyte2005() {
		// NTUAH1 0.3555
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2005_ADHOC, "NTUAH1"));
	}
	
	@Test
	public void approveBestBprefRunFromTable1ForTerabyte2006() {
		// uwmtFadTPFB 0.4251
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2006, "uwmtFadTPFB"));
	}
	
	@Test
	public void approveWorstBprefRunFromTable1Terabyte2006() {
		// arscDomAlog 0.1463
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2006, "arscDomAlog"));
	}
	
	@Test
	public void approveTHUADALLFromTable1Terabyte2006() {
		// THUADALL 0.3432
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2006, "THUADALL"));
	}
	
	@Test
	public void approveCoveoRun1FromTable1Terabyte2006() {
		// CoveoRun1 0.3886
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.TERABYTE_2006, "CoveoRun1"));
	}
}
