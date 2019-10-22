package de.webis.trec_ndd.trec_eval;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.Data;
import lombok.Getter;
import uk.ac.gla.terrier.jtreceval.gdeval;
import uk.ac.gla.terrier.jtreceval.ndeval;
import uk.ac.gla.terrier.jtreceval.trec_eval;

@Data
@Getter
public class TrecEvaluation {
	private final trec_eval trec_eval = new trec_eval();
	private final gdeval gdeval = new gdeval();
	private final ndeval ndeval = new ndeval();
	private final String runName;
	private final Path runFile;
	private final Path qrelFile;
	
	public double evaluateMeasureWithTrecEval(String measure, SharedTask task) {
		try {
			return trec_eval.evaluateMeasure(measure, qrelFile.toFile(), runFile.toFile(), task.argsForTrecEval());
		} catch(Exception e) {
			throw wrapInDetailedException(e);
		}
	}
	
	public double evaluateMeasureWithNdEval(String measure, SharedTask task) {
		return evaluateAllMeasuresWithNdEval().get(measure);
	}
	

	private Map<String, Double> evaluateAllMeasuresWithNdEval() {
		try {
			return ndeval.evaluate(new File(qrelFile.toFile().toString() +".ndeval"), runFile.toFile());
		} catch(Exception e) {
			throw wrapInDetailedException(e);
		}
	}

	public double err20() {
		return gdeval.err_20(qrelFile.toFile(), runFile.toFile());
	}
	
	public double err10() {
		return gdeval.err_10(qrelFile.toFile(), runFile.toFile());
	}
	
	public double ndcg20() {
		return gdeval.ndcg_20(qrelFile.toFile(), runFile.toFile());
	}
	
	public double ndcg10() {
		return gdeval.ndcg_10(qrelFile.toFile(), runFile.toFile());
	}
	
	private RuntimeException wrapInDetailedException(Exception e) {
		return new RuntimeException("Evaluation failed for run '" + runName
				+ "' with qrel: '" + qrelFile.toFile().toString() + "' and run-file '" + runFile.toFile().toString(),e);
	}
}
