package de.webis.trec_ndd.trec_collections;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Set;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

public class QrelReader {

	public List<Qrel> readFile(String filepath) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(filepath));
		String line;
		List<Qrel> results = new ArrayList<Qrel>();
		while((line=reader.readLine())!=null) {
			results.add(new Qrel(line));
		}
		reader.close();
		return results;
	}
	
	public List<QrelEqualWithoutScore> readFileQrelEqualWithoutScore(String filePath) throws IOException{
		return readFileQrelEqualWithoutScore(new File(filePath));
	}
	
	public List<QrelEqualWithoutScore> readFileQrelEqualWithoutScore(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		List<QrelEqualWithoutScore> results = new ArrayList<QrelEqualWithoutScore>();
		while((line=reader.readLine())!=null) {
			while(line.contains("  ")) line=line.replaceAll("  "," ");
			results.add(new QrelEqualWithoutScore(line));
		}
		reader.close();
		return results;
		
	}

	public Set<String> readJudgedDocumentIdsFromQrels(File file) throws IOException {
		return readJudgedDocumentIdsFromQrels(new FileInputStream(file));
	}
	
	public Set<String> readJudgedDocumentIdsFromQrels(InputStream inputStream) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		Set<String> results = new HashSet<String>();
		while((line=reader.readLine())!=null) {
			results.add(new Qrel(line).getDocumentID());
		}
		reader.close();
		return results;

		/*return new QueryJudgments(qrelsFile)
				.getQrels().values().stream()
				.map(Map::keySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
		*/
	}
}
