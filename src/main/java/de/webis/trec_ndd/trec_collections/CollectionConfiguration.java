package de.webis.trec_ndd.trec_collections;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

public interface CollectionConfiguration extends Serializable {

	public String getPathToCollection();
	public String getCollectionType();
	public String getDocumentGenerator();
	public List<SharedTask> getSharedTasks();

	@SneakyThrows
	public default Set<String> judgedDocumentIds() {
		Set<String> ret = new HashSet<>();
		
		for(String qrelResource : getQrelResourcesOfAllSharedTasks()) {
			InputStream resource = AnseriniCollectionReader.class.getResourceAsStream(qrelResource);
			ret.addAll(new QrelReader().readJudgedDocumentIdsFromQrels(resource));
		}
		
		return ret;
	}
	
	public default HashSet<String> documentIdsInRunFiles() {
		HashSet<String> ret = new HashSet<>();
		
		for(SharedTask sharedTask: getSharedTasks()) {
			ret.addAll(sharedTask.documentIdsInRunFiles());
		}
		
		return ret;
	}
	
	public default List<String> getQrelResourcesOfAllSharedTasks() {
		return getSharedTasks().stream()
				.map(SharedTask::getQrelResource)
				.collect(Collectors.toList());
	}
	
	@Getter
	@AllArgsConstructor
	public static enum TrecCollections implements CollectionConfiguration {
		CLUEWEB09(
			// compressed 4TB
			"/mnt/ceph/storage/corpora/corpora-thirdparty/corpus-clueweb09/",
			"ClueWeb09Collection",
			"JsoupGenerator",
			Collections.unmodifiableList(Arrays.asList(
				TrecSharedTask.WEB_2009,
				TrecSharedTask.WEB_2010,
				TrecSharedTask.WEB_2011,
				TrecSharedTask.WEB_2012,
				TrecSharedTask.SESSION_2010,
				TrecSharedTask.SESSION_2011,
				TrecSharedTask.SESSION_2012
			))
		),
		CLUEWEB12(
			"/mnt/ceph/storage/data-in-progress/kibi9872/clueweb12/",
			"ClueWeb12Collection",
			"JsoupGenerator",
			Collections.unmodifiableList(Arrays.asList(
					TrecSharedTask.WEB_2013,
					TrecSharedTask.WEB_2014,
					TrecSharedTask.SESSION_2013,
					TrecSharedTask.SESSION_2014
			))
		),
		GOV1(
			// compressed 4.6 GB
			"/mnt/nfs/webis20/corpora/corpora-thirdparty/corpora-trec/corpus-trec-web/DOTGOV/",
			"TrecwebCollection",
			"JsoupGenerator",
			Collections.unmodifiableList(Collections.emptyList())
		),
		GOV2(
			// compressed 81GB
			"/mnt/nfs/webis20/corpora/corpora-thirdparty/corpora-trec/corpus-trec-web/DOTGOV2/gov2-corpus/",
			"TrecwebCollection",
			"JsoupGenerator",
			Collections.unmodifiableList(Arrays.asList(
					TrecSharedTask.TERABYTE_2004,
					TrecSharedTask.TERABYTE_2005_ADHOC,
					TrecSharedTask.TERABYTE_2006
			))
		),
		GOV2_MQ(			// compressed 81GB
				"/mnt/ceph/storage/corpora/corpora-thirdparty/corpora-trec/corpus-trec-web/DOTGOV2/gov2-corpus/",
				"TrecwebCollection",
				"JsoupGenerator",
				Collections.unmodifiableList(Arrays.asList(
					TrecSharedTask.MILLION_QUERY_2007,
					TrecSharedTask.MILLION_QUERY_2008
				))
		),
		ROBUST04(
			"/mnt/ceph/storage/data-in-progress/kibi9872/robust-04/",
			"TrecCollection",
			"JsoupGenerator",
			Collections.unmodifiableList(Arrays.asList(TrecSharedTask.ROBUST_04))
		),
		CORE2018(
			"/mnt/nfs/webis20/data-in-progress/trec-2018-webis/WashingtonPost/WashingtonPost.v2/data/",
			"WashingtonPostCollection",
			"WapoGenerator",
			Collections.unmodifiableList(Arrays.asList(TrecSharedTask.CORE_2018))
		),
		CORE2017(
			"/mnt/nfs/webis20/data-in-progress/trec-2017-webis/data/nyt/",
			"NewYorkTimesCollection",
			"JsoupGenerator",
			Collections.unmodifiableList(Arrays.asList(TrecSharedTask.CORE_2017))
		);
		

		private final String pathToCollection,
							collectionType,
							documentGenerator;

		private final List<SharedTask> sharedTasks;
	}
	
	@Getter
	@AllArgsConstructor
	public static enum OtherCollections implements CollectionConfiguration {
		COMMON_CRAWL_2015_02("CommonCrawlCollection", "JsoupGenerator");
		
		private final String collectionType,
			documentGenerator;

		@Override
		public List<SharedTask> getSharedTasks() {
			return Collections.emptyList();
		}

		@Override
		public String getPathToCollection() {
			return null;
		}
	}
}
