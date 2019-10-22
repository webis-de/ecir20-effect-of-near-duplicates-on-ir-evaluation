package de.webis.trec_ndd.trec_collections;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class RunFilesOfSharedTaskIntegrationTest {
	@Test
	public void approveCore2018() {
		int expectedRunFiles = 72;
		int expectedHash = 1110435132;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec27/core/input.BM25-b-BoW.gz",
				"trec27/core/input.webis-bm25f.gz"
		);
		
		List<String> runFiles = TrecSharedTask.CORE_2018.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveCore2017() {
		int expectedRunFiles = 75;
		int expectedHash = -1313041334;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec26/core/input.UWatMDS_BFuse.gz",
				"trec26/core/input.WCrobust0405.gz",
				"trec26/core/input.webis_baseline2.gz"
		);
		
		List<String> runFiles = TrecSharedTask.CORE_2017.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveTerabyte2004() {
		int expectedRunFiles = 70;
		int expectedHash = -1829180574;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec13/terabyte/input.DcuTB04Base.gz",
				"trec13/terabyte/input.zetplain.gz",
				"trec13/terabyte/input.humT04l.gz"
		);
		
		List<String> runFiles = TrecSharedTask.TERABYTE_2004.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveTerabyte2005() {
		int expectedRunFiles = 58;
		int expectedHash = -2010098114;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec14/terabyte.adhoc/input.DCU05ABM25.gz",
				"trec14/terabyte.adhoc/input.zetdirhoc.gz",
				"trec14/terabyte.adhoc/input.humT05xle.gz",
				"trec14/terabyte.adhoc/input.DCU05AWTF.gz"
		);
		
		List<String> runFiles = TrecSharedTask.TERABYTE_2005_ADHOC.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveTerabyte2006() {
		int expectedRunFiles = 80;
		int expectedHash = -349783679;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec15/terabyte-adhoc/input.AMRIMtp20006.gz",
				"trec15/terabyte-adhoc/input.zetamerg.gz",
				"trec15/terabyte-adhoc/input.AMRIMtp20006.gz",
				"trec15/terabyte-adhoc/input.hedge0.gz"
		);
		
		List<String> runFiles = TrecSharedTask.TERABYTE_2006.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2009() {
		int expectedRunFiles = 71;
		int expectedHash = -1232077917;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec18/web.adhoc/input.watwp.gz",
				"trec18/web.adhoc/input.uvamrftop.gz",
				"trec18/web.adhoc/input.UMHOOsd.gz",
				"trec18/web.adhoc/input.ICTNETADRun3.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2009.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2010() {
		//FIXME: the paper reports 55, but on the webpage are 56, so I stick with 56 at the moment.
		int expectedRunFiles = 56;
		int expectedHash = -969190245;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec19/web.adhoc/input.york10wA2.gz",
				"trec19/web.adhoc/input.sztaki1.gz",
				"trec19/web.adhoc/input.msrsv3.gz",
				"trec19/web.adhoc/input.cmuWiki10.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2010.runFiles();
		Collections.sort(runFiles);

		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));

		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2011() {
		int expectedRunFiles = 37;
		int expectedHash = -2019917221;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec20/web.adhoc/input.DFalah11.gz",
				"trec20/web.adhoc/input.uogTrA45Vm.gz",
				"trec20/web.adhoc/input.msrsv2011a2.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2011.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2012() {
		int expectedRunFiles = 28;
		int expectedHash = 172625750;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec21/web.adhoc/input.uogTrA44xi.gz",
				"trec21/web.adhoc/input.irra12c.gz",
				"trec21/web.adhoc/input.qutwb.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2012.runFiles();
		Collections.sort(runFiles);

		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2013() {
		int expectedRunFiles = 34;
		int expectedHash = -1557530286;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec22/web.adhoc/input.clustmrfaf.gz",
				"trec22/web.adhoc/input.UJS13LCRAd2.gz",
				"trec22/web.adhoc/input.ut22xact.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2013.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	@Test
	public void approveWeb2014() {
		int expectedRunFiles = 30;
		int expectedHash = 1078203589;
		List<String> sampleExpectedRunFiles = runFiles(
				"trec23/web.adhoc/input.UDInfoWebLES.gz",
				"trec23/web.adhoc/input.SNUMedinfo12.gz",
				"trec23/web.adhoc/input.webisWt14axAll.gz",
				"trec23/web.adhoc/input.wistud.runB.gz"
		);
		
		List<String> runFiles = TrecSharedTask.WEB_2014.runFiles();
		Collections.sort(runFiles);
		
		Assert.assertEquals(expectedRunFiles, runFiles.size());
		Assert.assertTrue(runFiles.containsAll(sampleExpectedRunFiles));
		Assert.assertEquals(expectedHash, runFiles.hashCode());
	}
	
	private static List<String> runFiles(String...files) {
		return Stream.<String>of(files)
			.map(f -> "/mnt/nfs/webis20/data-in-progress/trec-system-runs/" + f)
			.collect(Collectors.toList());
	}
}
