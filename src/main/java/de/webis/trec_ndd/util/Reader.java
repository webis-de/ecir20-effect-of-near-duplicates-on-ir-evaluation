package de.webis.trec_ndd.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import lombok.SneakyThrows;

public class Reader<T> implements Iterator<T>, Iterable<T>{
	
	private Class<T> readerType;
	private BufferedReader[] readerList;
	private BufferedReader currentReader;
	private int currentReaderIndex=0;
	private String lastLine=null;
	private File fileToBeRead;
	
	private Reader(Reader<T> r) throws IOException{
		init(r.fileToBeRead, r.readerType);
	}

	
	public Reader(File file, Class<T> cls) throws IOException{
		init(file,cls);
	}
	
	public Reader(String sfile, Class<T> cls) throws IOException{
		init(new File(sfile),cls);
	}
	
	public void init(File file, Class<T> cls) throws IOException{
		this.fileToBeRead=file;
		readerType=cls;
		if(!file.exists()) throw new FileNotFoundException();
		if(file.isDirectory()) {
			File[] fileList;
			if((fileList=file.listFiles())==null || fileList.length==0) throw new IOException("no groups found");
			this.readerList=new BufferedReader [fileList.length];
			for(int i=0; i<fileList.length; i++) {
				this.readerList[i]=new BufferedReader(new FileReader(fileList[i]));
			}
		}else {
			this.readerList=new BufferedReader[1];
			this.readerList[0]=new BufferedReader(new FileReader(file));
		}
		this.currentReader=this.readerList[0];
		this.lastLine=this.currentReader.readLine();
	}
	
	@Override
	public boolean hasNext() {
		return lastLine!=null;
	}

	@Override
	@SneakyThrows
	public T next() {
		T res=this.readerType.getDeclaredConstructor(String.class).newInstance(this.lastLine);
		this.lastLine=currentReader.readLine();
		while(this.lastLine==null && this.currentReaderIndex+1<this.readerList.length) {
			this.currentReader=this.readerList[++this.currentReaderIndex];
			this.lastLine=currentReader.readLine();
		}
		return res;
	}

	@Override
	public Iterator<T> iterator() {
		// TODO Auto-generated method stub
		return this;
	}
	
	@SneakyThrows
	public void close() {
		for(int i=0; i<this.readerList.length; i++) {
			this.readerList[i].close();
		}
	}
	
	@SneakyThrows
	public ArrayList<T> getArrayList(){
		ArrayList <T> res = new ArrayList<T>();
		Reader<T> reader = new Reader<T>(this);
		for(T element: reader) {
			res.add(element);
		}
		reader.close();
		return res;
	}
	
	@SneakyThrows
	public Collection<T> getCollection(){
		return (Collection <T>) getArrayList();
	}
}
