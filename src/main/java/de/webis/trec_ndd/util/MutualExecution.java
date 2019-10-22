package de.webis.trec_ndd.util;

import de.webis.trec_ndd.spark.QrelConsistentMaker;
import de.webis.trec_ndd.spark.RunResultDeduplicator;

public class MutualExecution {

	public static Boolean shouldExecuteTogether(QrelConsistentMaker qrel, RunResultDeduplicator dedup) {
		return !"maxValueAllDuplicateDocsIrrelevant".equals(qrel.name()) || !"base".equals(dedup.name());
	}

}
