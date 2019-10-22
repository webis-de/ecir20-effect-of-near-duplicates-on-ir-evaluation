package de.webis.trec_ndd.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;

import de.webis.trec_ndd.spark.DocumentGroup;
import de.webis.trec_ndd.util.Reader;
import lombok.SneakyThrows;

public class ReaderTest {
	
	@Test
	@SneakyThrows
	public void NonExistingFile() {
		try {
			new Reader<DocumentGroup>("./src/test/ressources/DocumentGroupReader/nonexisting",DocumentGroup.class);
			Assert.assertTrue(false);
		}catch(Exception e) {
			if(e instanceof FileNotFoundException) Assert.assertTrue(true);
			else throw e;
		}
	}
	
	@Test
	@SneakyThrows
	public void File() {
		Reader<DocumentGroup> dgr = new Reader<DocumentGroup>("./src/test/resources/DocumentGroupReader/OneFile/group0",DocumentGroup.class);
		List<DocumentGroup> list =new ArrayList<DocumentGroup>();
		while(dgr.hasNext()) {
			list.add(dgr.next());
		}
		Approvals.verifyAsJson(list);
	}
	
	@Test
	@SneakyThrows
	public void EmptyFolder() {
		try {
			java.io.File emptyFolder = Files.createTempDir(); 
			new Reader<DocumentGroup>(emptyFolder.getAbsolutePath(),DocumentGroup.class);
			Assert.assertTrue(false);
		}catch(Exception e) {
			if(e instanceof IOException && e.getMessage()!=null && e.getMessage().equals("no groups found")) Assert.assertTrue(true);
			else throw e;
		}
	}
	
	@Test
	@SneakyThrows
	public void FolderWithOneFile() {
		Reader<DocumentGroup> dgr = new Reader<DocumentGroup>("./src/test/resources/DocumentGroupReader/OneFile",DocumentGroup.class);
		List<DocumentGroup> list =new ArrayList<DocumentGroup>();
		while(dgr.hasNext()) {
			list.add(dgr.next());
		}
		Approvals.verifyAsJson(list);
	}
	
	@Test
	@SneakyThrows
	public void FolderWithMoreThanOneFile() {
		Reader<DocumentGroup> dgr = new Reader<DocumentGroup>("./src/test/resources/DocumentGroupReader/MoreThanOneFile",DocumentGroup.class);
		List<DocumentGroup> list =new ArrayList<DocumentGroup>();
		while(dgr.hasNext()) {
			list.add(dgr.next());
		}
		list.sort(new Comparator<DocumentGroup>() {
		    public int compare(DocumentGroup obj1, DocumentGroup obj2) {
		        return obj1.getHash().compareTo(obj2.getHash());
		    }
		});
		Approvals.verifyAsJson(list);
	}

}
