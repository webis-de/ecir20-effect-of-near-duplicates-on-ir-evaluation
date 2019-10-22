package de.webis.trec_ndd.spark;

import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.util.Reader;
import lombok.SneakyThrows;

public class RunResultDeduplicatorTest {

		@Test
		@SneakyThrows
		public void testDeduplicateRunResults() {
			Approvals.verify(deduplicate("./src/test/resources/RunResultDeduplicator/groupFile","./src/test/resources/RunResultDeduplicator/runFile"));
		}
		
		@Test
		@SneakyThrows
		public void testDeduplicateRunWithMultipleTopics() {
			Approvals.verify(deduplicate("./src/test/resources/RunResultDeduplicator/groupFile", "./src/test/resources/RunResultDeduplicator/runFileWithMultipleTopics"));
		}
		
		@SneakyThrows
		private static String deduplicate(String groupPath, String runFile){
			Reader<DocumentGroup> docReader = new Reader<DocumentGroup>(groupPath,DocumentGroup.class);
			Reader<RunLine> runReader = new Reader<RunLine>(runFile,RunLine.class);
			return RunResultDeduplicator.removeDuplicates.deduplicateRun(runReader.getArrayList(), docReader.getArrayList())
					.getDeduplicatedRun()
					.stream()
					.map(RunLine::toString)
					.collect(Collectors.joining("\n"));
		}
}
