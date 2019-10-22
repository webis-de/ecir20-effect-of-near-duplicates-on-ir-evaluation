package de.webis.trec_ndd.trec_collections;

import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@Accessors(chain = true)
@SuppressWarnings("serial")
public class Qrel implements Serializable{
    private int score;
    private int topicNumber;
    private String documentID;
    
	public Qrel(String line) {
        String[] args = line.split("[\\s\\t]+");
        
        if (args.length == 5) {
        	//prels, like: 1 clueweb09-en0003-55-31884 0 0 1
            topicNumber = Integer.parseInt(args[0]);
            documentID = args[1];
            //FIXME: what is the score for prels?
            score = Integer.MIN_VALUE;
        }
        else {
            topicNumber = Integer.parseInt(args[0]);
            //skipping 1 because it is irrelevant -> has always 0 as value wehen printed
            documentID = args[2];
            score = Integer.parseInt(args[3]);	
        }
    }
	

	
	public String toString() {
		return new StringBuilder().append(topicNumber).append(" 0 ").append(documentID).append(' ').append(score).toString();
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof Qrel) {
			return this.documentID.equals(((Qrel) o).documentID) && this.score==((Qrel)o).score && this.topicNumber==((Qrel)o).topicNumber;
		}
		else return false;
	}
}

