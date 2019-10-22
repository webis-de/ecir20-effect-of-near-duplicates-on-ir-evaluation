package de.webis.trec_ndd.util;

import java.util.Arrays;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.util.NGramms.Word8Gramm;

public class NGrammsTest {

	@Test
	public void checkThatToFewTokensAreValidInput() {
		List<Word8Gramm> expected = Arrays.asList();
		List<Word8Gramm> actual = NGramms.build8Gramms(Arrays.asList("1", "2", "3", "4", "6", "7", "8"));
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void checkThatToEmptyListIsValidInput() {
		List<Word8Gramm> expected = Arrays.asList();
		List<Word8Gramm> actual = NGramms.build8Gramms(Arrays.asList());
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void extract8GrammsFromTextOfLength8() {
		List<Word8Gramm> actual = NGramms.build8Gramms(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"));
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void extract8GrammsFromTextOfLength10() {
		List<Word8Gramm> actual = NGramms.build8Gramms(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void extract8GrammsFromTextOfLength12() {
		List<Word8Gramm> actual = NGramms.build8Gramms(Arrays.asList("a", "B", "c", "D", "e", "F", "g", "h", "i", "j", "klm", "nop"));
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void extract8GrammsFromText() {
		List<Word8Gramm> actual = NGramms.build8Gramms("a B c D e F g h i j klm nop");
		
		Approvals.verifyAsJson(actual);
	}
}
