package de.webis.trec_ndd.trec_eval;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;

import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.trec_collections.Qrel;
import lombok.SneakyThrows;

public class TrecEvaluator {
	@SneakyThrows
	public TrecEvaluation evaluate(String name, Path qrelFile, Path runFile) {
		if(runFile.getFileName().toString().endsWith(".gz")) {
			return evaluate(name, qrelFile, unzippedFile(runFile));
		}
		
		return new TrecEvaluation(name, runFile, qrelFile);
	}
	
	@SneakyThrows
	private static Path unzippedFile(Path p) {
		Path ret = nonExistingTmpFileDeletedOnExit("tmp-unzipped").toPath();
		InputStream content = RunLine.openRunFile(p);
		
		Files.copy(content, ret);
		
		return ret;
	}
	
	@SneakyThrows
	public TrecEvaluation evaluate(String name, Collection<? extends Qrel> qrels, Collection<RunLine> runlines) {
		Path qrelFile = nonExistingTmpFileDeletedOnExit("qrels").toPath();
		Path runFile = nonExistingTmpFileDeletedOnExit(".run").toPath();
		
		Files.write(qrelFile, qrels.stream().map(Qrel::toString).collect(Collectors.toList()));
		Files.write(runFile, runlines.stream().map(RunLine::toString).collect(Collectors.toList()));
		
		return evaluate(name, qrelFile, runFile);
	}
	
	@SneakyThrows
	private static File nonExistingTmpFileDeletedOnExit(String prefix) {
		Path ret = Files.createTempDirectory(prefix);
		ret.toFile().deleteOnExit();
		
		return ret.resolve(prefix).toAbsolutePath().toFile();
	}
}
