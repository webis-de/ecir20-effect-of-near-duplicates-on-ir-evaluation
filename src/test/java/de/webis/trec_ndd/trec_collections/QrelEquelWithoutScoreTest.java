package de.webis.trec_ndd.trec_collections;

import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.spark.RunLine;

public class QrelEquelWithoutScoreTest {
	@Test
	public void testQrelWithoutScore() {
		QrelEqualWithoutScore q1 = new QrelEqualWithoutScore("51 0 clueweb09-en0003-93-25577 0");
		QrelEqualWithoutScore q2 = new QrelEqualWithoutScore("51 0 clueweb09-en0003-93-25577 1");
		Assert.assertTrue(q1.equals(q2));
		Assert.assertTrue(q1.equals(new Qrel("51 0 clueweb09-en0003-93-25577 1")));
		HashSet<QrelEqualWithoutScore> a =new HashSet<QrelEqualWithoutScore>();
		a.add(q1);
		a.add(q2);
		Assert.assertTrue(a.size()==1);
	}
	
	@Test
	public void testForCommaAsTousendSeperator() {
		RunLine r = new RunLine("750	Q0	GX250-63-10199615	1,000	3.400966	mpi04tb81");
		
		Assert.assertEquals(1000, r.getRank());
		Assert.assertEquals(750, r.getTopic());
		Assert.assertEquals("GX250-63-10199615", r.getDoucmentID());
	}
	
	@Test
	public void testThatNonDecreasableRankDoesNotFail() {
		RunLine r = new RunLine("1 Q0 clueweb09-en0002-89-01498 1.4929032370821175e-9 5.916468 SIEL09");
		
		Assert.assertEquals(1, r.getTopic());
		Assert.assertEquals("clueweb09-en0002-89-01498", r.getDoucmentID());
		
		try {
			r.getRank();
			Assert.fail("Should not reach this place");
		} catch(Exception e) { }
		
		RunLine newRunline = r.createNewWithRankMinus(-12);
		
		try {
			newRunline.getRank();
			Assert.fail("Should not reach this place");
		} catch(Exception e) { }

		Assert.assertEquals(1, newRunline.getTopic());
		Assert.assertEquals("clueweb09-en0002-89-01498", newRunline.getDoucmentID());
	}
}
