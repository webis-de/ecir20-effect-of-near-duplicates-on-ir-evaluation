package de.webis.trec_ndd.trec_collections;

import java.io.Serializable;

import lombok.experimental.Accessors;

@Accessors(chain = true)
@SuppressWarnings("serial")
public class QrelEqualWithoutScore extends Qrel implements Serializable{

	public QrelEqualWithoutScore() {
		
	}
	
	public QrelEqualWithoutScore(String line) {
		super(line);
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof Qrel || o instanceof QrelEqualWithoutScore) {
			return this.getDocumentID().equals(((Qrel) o).getDocumentID()) && this.getTopicNumber()==((Qrel)o).getTopicNumber();
		}
		else return false;
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder().append(getTopicNumber()).append(getDocumentID()).toString().hashCode();
	}
	
	@Override
	public String toString() {
		return super.toString();
	}
	
	public QrelEqualWithoutScore copy() {
		QrelEqualWithoutScore ret = new QrelEqualWithoutScore();
		ret.setDocumentID(this.getDocumentID());
		ret.setTopicNumber(this.getTopicNumber());
		ret.setScore(this.getScore());
		
		return ret;
	}
	
}