package de.webis.trec_ndd.trec_collections;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_eval.EvaluationMeasure;
import lombok.SneakyThrows;

public class RankingResultParsingTest {

	@Test
	public void approveRankingResultsForTrec19Example() {
		SharedTask sharedTask = sharedTaskWithRunFileDirectory("src/test/resources/data/sample-run-files-trec-19-web");
		Approvals.verifyAsJson(runFileKpis(sharedTask));
	}
	
	@Test
	public void approveRankingResultsForArtificialCore2018Example() {
		SharedTask sharedTask = sharedTaskWithRunFileDirectory("src/test/resources/data/artificial-sample-run-files-core-2018");
		
		Approvals.verifyAsJson(runFileKpis(sharedTask));
	}
	
	@Test
	public void approveDocumentIdsInRunFilesForTrec19Example() {
		SharedTask sharedTask = sharedTaskWithRunFileDirectory("src/test/resources/data/sample-run-files-trec-19-web");
		String sortedIds = sharedTask.documentIdsInRunFiles().stream().sorted()
				.collect(Collectors.joining("\n"));
		
		Approvals.verify(sortedIds);
	}
	
	@Test
	public void approveDocumentIdsInRunFilesForArtificialCore2018Example() {
		SharedTask sharedTask = sharedTaskWithRunFileDirectory("src/test/resources/data/artificial-sample-run-files-core-2018");
		String sortedIds = sharedTask.documentIdsInRunFiles().stream().sorted()
				.collect(Collectors.joining("\n"));
		
		Approvals.verify(sortedIds);
	}
	
	private static Map<String, Object> runFileKpis(SharedTask sharedTask) {
		Map<String, Object> ret = new LinkedHashMap<>();
		
		sharedTask.rankingResults()
			.sorted((a,b) -> a.getLeft().compareTo(b.getLeft()))
			.forEach(result -> {
			Map<String, Object> kpi = new HashMap<>();
			kpi.put("run-lines", result.getRight().size());
			kpi.put("run-lines-hash", result.getRight().hashCode());
			kpi.put("first-run-line", result.getRight().get(0).toString());
			kpi.put("last-run-line", result.getRight().get(result.getRight().size()-1).toString());
			
			ret.put(result.getLeft(), kpi);
		});
		
		return ret;
	}
	
	public static SharedTask sharedTaskWithRunFileDirectory(String runFileDirectory) {
		return new SharedTask() {
			@Override
			public String getQrelResource() {
				return null;
			}

			@Override
			public List<EvaluationMeasure> getOfficialEvaluationMeasures() {
				return null;
			}
			
			@Override
			public List<EvaluationMeasure> getInofficialEvaluationMeasures() {
				return null;
			}

			@Override
			@SneakyThrows
			public List<String> runFiles() {
				return Files.list(Paths.get(runFileDirectory))
						.map(Object::toString)
						.collect(Collectors.toList());
			}
			
			@Override
			public String name() {
				return null;
			}

			@Override
			public Map<String, Map<String, String>> topicNumberToTopic() {
				return null;
			}

			@Override
			public String getGroupFingerprintResource() {
				return null;
			}
		};
	}
}
