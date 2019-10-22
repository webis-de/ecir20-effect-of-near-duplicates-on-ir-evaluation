package de.webis.trec_ndd.trec_collections;

import java.util.List;

public interface CollectionReader {
	public List<CollectionDocument> extractJudgedRawDocumentsFromCollection();
	
	public List<CollectionDocument> extractRunFileDocumentsFromsCollection();

	public List<CollectionDocument> extractRawDocumentsFromCollection();
}
