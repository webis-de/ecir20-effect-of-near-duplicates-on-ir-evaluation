package de.webis.trec_ndd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import de.webis.trec_ndd.trec_collections.SharedTask;
import lombok.Data;
import lombok.SneakyThrows;

public interface RunResultDeduplicator {

	public static final RunResultDeduplicator 
		base = buildDeduplicator((a,b) -> new DeduplicationResult(a, Collections.emptyMap()), "base"),
		removeDuplicates = buildDeduplicator(Internals::deduplicateRunResults, "removeDuplicates"),
		duplicatesMarkedIrrelevant = buildDeduplicator(Internals::duplicatesMarkedIrrelevant, "duplicatesMarkedIrrelevant");
	
	public DeduplicationResult deduplicateRun(List<RunLine> runLines, Collection<DocumentGroup> docGroups);

	public String name();
	
	public static List<RunResultDeduplicator> all(SharedTask task) {
		return Arrays.asList(
			base,
			removeDuplicates,
			duplicatesMarkedIrrelevant
		);
	}
	public static class Internals{
		
		public static DeduplicationResult duplicatesMarkedIrrelevant(List<RunLine> runLines, Collection<DocumentGroup> docGroups) {
			Map<Integer, Set<String>> topicToDocumentsToMarkAsIrrelevant = new HashMap<>();
			HashMap<RunLine, String> runLineMapGroup = helper(runLines, docGroups);
			HashSet<String> doneGroups = new HashSet<String>();
			ArrayList<RunLine> output = new ArrayList<RunLine>();
			int currentTopic=-1;
			for(RunLine rl: runLines) {
				if(currentTopic!=rl.getTopic()) {
					currentTopic=rl.getTopic();
				}
				if(!runLineMapGroup.containsKey(rl)) {
					output.add(rl.copy());
					continue;
				}
				
				String docGroupWithTopic = runLineMapGroup.get(rl)+rl.getTopic();
				
				if(!doneGroups.contains(docGroupWithTopic)){
					doneGroups.add(docGroupWithTopic);
					output.add(rl.copy());
				}
				else{
					if(!topicToDocumentsToMarkAsIrrelevant.containsKey(currentTopic)) {
						topicToDocumentsToMarkAsIrrelevant.put(currentTopic, new HashSet<>());
					}
					topicToDocumentsToMarkAsIrrelevant.get(currentTopic).add(rl.getDoucmentID());
					output.add(rl.copy());
				}
			}
			
			return new DeduplicationResult(output, topicToDocumentsToMarkAsIrrelevant); 
		}		
		
		private static HashMap<RunLine, String> helper(List<RunLine> runLines, Collection<DocumentGroup> docGroups) {
			
			// Read RunLine and map runline.id to runline
			HashMap<String,ArrayList<RunLine>> idMapRunLines = new HashMap<String,ArrayList<RunLine>>();
			for(RunLine rl : runLines) {
				if(!idMapRunLines.containsKey(rl.getDoucmentID())) idMapRunLines.put(rl.getDoucmentID(),new ArrayList<RunLine>());
				idMapRunLines.get(rl.getDoucmentID()).add(rl);
			}
			//Map RunLine to Group
			HashMap<RunLine, String> runLineMapGroup = new HashMap<RunLine,String>();
			for(DocumentGroup currentGroup: docGroups)
				for(String id: currentGroup.ids) {
					if(!idMapRunLines.containsKey(id)) continue;
					for(RunLine rl: idMapRunLines.get(id)) 
						runLineMapGroup.put(rl, currentGroup.hash);
				}
			return runLineMapGroup;
		}
		
		@SneakyThrows
		public static DeduplicationResult deduplicateRunResults(List<RunLine> runLines, Collection<DocumentGroup> docGroups) {
			Map<Integer, Set<String>> topicToDocumentsToMarkAsIrrelevant = new HashMap<>();
			HashMap<RunLine, String> runLineMapGroup = helper(runLines, docGroups);
			//copy runfile but lines with the same group, when a line of the group is already written
			HashSet<String> doneGroups = new HashSet<String>();
			int currentTopic=-1;
			int rankDiff=0;
			ArrayList<RunLine> output = new ArrayList<RunLine>();
			for(RunLine rl: runLines) {
				if(currentTopic!=rl.getTopic()) {
					currentTopic=rl.getTopic();
					rankDiff=0;
				}
				if(runLineMapGroup.containsKey(rl)) {
					if(!doneGroups.contains(runLineMapGroup.get(rl)+rl.getTopic())){
						doneGroups.add(runLineMapGroup.get(rl)+rl.getTopic());
					}
					else{
						rankDiff++;
						if(!topicToDocumentsToMarkAsIrrelevant.containsKey(currentTopic)) {
							topicToDocumentsToMarkAsIrrelevant.put(currentTopic, new HashSet<>());
						}
						topicToDocumentsToMarkAsIrrelevant.get(currentTopic).add(rl.getDoucmentID());
						
						continue; 
					}
				}
				output.add(rl.createNewWithRankMinus(rankDiff));
			}
			
			return new DeduplicationResult(output, topicToDocumentsToMarkAsIrrelevant); 
		}
	}
	
	@Data
	public static class DeduplicationResult {
		private final List<RunLine> deduplicatedRun;
		private final Map<Integer, Set<String>> topicToDocumentIdsToMakeIrrelevant;
	}

	public static RunResultDeduplicator buildDeduplicator(BiFunction<List<RunLine>, Collection<DocumentGroup>, DeduplicationResult> strategy, String name) {
		return new RunResultDeduplicator() {
			@Override
			public DeduplicationResult deduplicateRun(List<RunLine> runLines, Collection<DocumentGroup> docGroups) {
				return strategy.apply(runLines, docGroups);
			}

			@Override
			public String name() {
				return name;
			}
		};
	}
}
