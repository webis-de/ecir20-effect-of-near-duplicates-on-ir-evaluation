package de.webis.trec_ndd.trec_eval;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.webis.trec_ndd.trec_collections.SharedTask;
import org.apache.commons.collections.list.UnmodifiableList;

import lombok.SneakyThrows;

public interface EvaluationMeasure {
	public static final EvaluationMeasure
		ALPHA_NDCG_CUT_10 = (eval, task) -> eval.evaluateMeasureWithNdEval("alpha-nDCG@10", task),
		ALPHA_NDCG_CUT_20 = (eval, task) -> eval.evaluateMeasureWithNdEval("alpha-nDCG@20", task),
		BPREF = (eval, task) -> eval.evaluateMeasureWithTrecEval("bpref", task),
		ERR_10 = (eval, task) -> eval.err10(),
		ERR_20 = (eval, task) -> eval.err20(),
		ERR_IA_10 = (eval, task) -> eval.evaluateMeasureWithNdEval("ERR-IA@10", task),
		ERR_IA_20 = (eval, task) -> eval.evaluateMeasureWithNdEval("ERR-IA@20", task),
//		E_MAP = (eval, task) -> (double) (Object) "TODO: ADD E_MAP",
//		E_P_5 = (eval, task) -> (double) (Object) "TODO: ADD E_P_5",
//		E_P_10 = (eval, task) -> (double) (Object) "TODO: ADD E_P_0",
//		E_P_20 = (eval, task) -> (double) (Object) "TODO: ADD E_P_20",
//		INF_AP = (eval, task) -> (double) (Object) "TODO: ADD INF_AP",
		MAP = (eval, task) -> eval.evaluateMeasureWithTrecEval("map", task),
		NDCG = (eval, task) -> eval.evaluateMeasureWithTrecEval("ndcg", task),
		NDCG_CUT_20 = (eval, task) -> eval.ndcg20(), //eval.evaluateMeasureWithTrecEval("ndcg_cut.20", task),
		NDCG_CUT_10 = (eval, task) -> eval.ndcg10(),
		NRBP = (eval, task) -> eval.evaluateMeasureWithNdEval("NRBP", task),
		P_10 = (eval, task) -> eval.evaluateMeasureWithTrecEval("P.10", task),
		P_20 = (eval, task) -> eval.evaluateMeasureWithTrecEval("P.20", task)
				
	;
	
	public Double evaluate(TrecEvaluation evaluation, SharedTask task);
	
	public static final List<EvaluationMeasure> all = Internals.getInstances();
	
	public default String name() {
		return Internals.names.get(this);
	}
	
	public static class Internals{
		public static Map<EvaluationMeasure, String> names = initNames();
		
		@SneakyThrows
		public static Map<EvaluationMeasure, String> initNames(){
			return Arrays.asList(EvaluationMeasure.class.getDeclaredFields())
				.stream()
				.filter(Internals::isInstanceOfThisClass)
				.collect(Collectors.toMap(Internals::convertField , f-> f.getName()));
		}
		
		@SuppressWarnings("unchecked")
		public static List<EvaluationMeasure> getInstances() {
			return UnmodifiableList.decorate(Arrays.asList(EvaluationMeasure.class.getDeclaredFields()).stream()
					.filter(Internals::isInstanceOfThisClass)
					.map(Internals::convertField)
					.collect(Collectors.toList()));
		}
		
		@SneakyThrows
		static boolean isInstanceOfThisClass(Field f) {
			return f.get(EvaluationMeasure.class) instanceof EvaluationMeasure;
		}
		
		@SneakyThrows
		static EvaluationMeasure convertField(Field f) {
			return (EvaluationMeasure) f.get(EvaluationMeasure.class);
		}
	}
	
	
	

}
