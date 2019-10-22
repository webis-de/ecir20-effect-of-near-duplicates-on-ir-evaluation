package de.webis.trec_ndd.trec_eval;

import org.junit.Assert;
import org.junit.Test;

public class EvaluationMeasureTest {

	@Test
	public void test() {
		Assert.assertTrue("MAP".equals(EvaluationMeasure.MAP.name()));
	}
}
