package de.webis.trec_ndd.trec_eval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Ignore;
import org.junit.Test;

import avro.shaded.com.google.common.collect.ImmutableMap;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;

public class EvaluationOnClueWeb09Test {
	
	@Test
	@Ignore
	public void approveBestwatwpRunFromTable2From2009() {
		// watwp 0.043362
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2009, "watwp"));
	}
	
	@Test
	@Ignore
	public void approveWorstMapRunFromTable2From2009() {
		// 
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2009, ""));
	}
	
	@Test
	@Ignore
	public void approveXXXRunFromTable2From2009() {
		// 
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2009, ""));
	}
	
	@Test
	@Ignore
	public void approveXYXRunFromTable2From2009() {
		// 
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2009, ""));
	}
	
	@Test
	public void approveBestMapRunFromTable2From2010() {
		// cmuWiki10 0.157
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2010, "cmuWiki10"));
	}
	
	@Test
	public void approveWorstMapRunFromTable2From2010() {
		// UMa10IASF 0.080
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2010, "UMa10IASF"));
	}
	
	@Test
	public void approveIvoryL2RbFromTable2From2010() {
		// IvoryL2Rb 0.134
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2010, "IvoryL2Rb"));
	}
	
	@Test
	public void approveTHUIR10QaHtRunFromTable2From2010() {
		// THUIR10QaHt 0.128
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2010, "THUIR10QaHt"));
	}
	
	@Test
	public void approveBestERR_20RunFromTable2From2011() {
		// ICTNET11ADR3 0.157
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2011, "ICTNET11ADR3"));
	}
	
	@Test
	public void approveWorstERR_20RunFromTable2From2011() {
		// uwBAadhoc 0.119
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2011, "uwBAadhoc"));
	}
	
	@Test
	public void approvemsrsv2011a3FromTable2From2011() {
		// msrsv2011a3 0.143
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2011, "msrsv2011a3"));
	}
	
	
	@Test
	public void approveDFalah11RunFromTable2From2011() {
		// DFalah11 0.122
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2011, "DFalah11"));
	}
	
	@Test
	public void approveBestERR_20RunFromTable2From2012() {
		// uogTrA44xi 0.313
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2012, "uogTrA44xi"));
	}
	
	@Test
	public void approveWorstERR_20RunFromTable2From2012() {
		// qutwb 0.166
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2012, "qutwb"));
	}
	
	@Test
	public void approveQUTparaBlineFromTable2From2012() {
		// QUTparaBline 0.290
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2012, "QUTparaBline"));
	}
	
	@Test
	public void approveirra12cRunFromTable2From2012() {
		// irra12c 0.290
		Approvals.verifyAsJson(evaluateAllOfficialMeasuresForRun(TrecSharedTask.WEB_2012, "irra12c"));
	}
	
	public static Object evaluateAllOfficialMeasuresForRun(SharedTask sharedTask, String run) {
		Map<String, String> ret = new HashMap<>();
		Path runFile = runFile(run, sharedTask);
		
		for(EvaluationMeasure evalMeasure: sharedTask.getOfficialEvaluationMeasures()) {
			ret.put(evalMeasure.name(), evalSec(evalMeasure, runFile, sharedTask));
		}
		
		return ImmutableMap.<String, Object>builder()
				.put(run, ret)
				.build();
	}
	
	public static Path runFile(String run, SharedTask task) {
		List<String> candidateRunFiles = task.runFiles().stream()
				.filter(p -> p.contains(run))
				.collect(Collectors.toList());
		
		if(candidateRunFiles.size() != 1) {
			throw new RuntimeException("Cant handle run: '"+ run +"' for shared task '"+ task +"'.");
		}
		
		return Paths.get(candidateRunFiles.get(0));
	}
	
	public static String evalSec(EvaluationMeasure measure, Path run, SharedTask task) {
		try {
			return eval(measure, run, task);
		} catch(Exception e) { }
		
		return "EXCEPTION-DURING-COMPUTATION!";
	}
	
	@SneakyThrows
	public static String eval(EvaluationMeasure measure, Path run, SharedTask task) {
		TrecEvaluation eval = new TrecEvaluator().evaluate(run.toFile().toString(), task.getQrelResourceAsFile(), run);
		return String.format("%1$,.3f", measure.evaluate(eval, task));
	}
}
