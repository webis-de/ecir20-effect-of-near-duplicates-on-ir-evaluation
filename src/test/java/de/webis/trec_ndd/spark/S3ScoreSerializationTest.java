package de.webis.trec_ndd.spark;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;

public class S3ScoreSerializationTest {
	
	private static final String EXAMPLE_S3SCORE = "{\"idPair\":{\"left\":\"left\",\"right\":\"right\",\"value\":\"right\",\"key\":\"left\"},\"commonNGramms\":3,\"s3Score\":0.6,\"chunksInA\":0.2,\"chunksInB\":0.0,\"meanChunks\":0.5}"; 
	
	@Test
	public void checkSerializationOfS3Score() {
		S3Score score = exampleS3Score();
		
		Assert.assertEquals(EXAMPLE_S3SCORE, score.toString());
	}
	
	@Test
	public void checkDeserializationOfS3Score() {
		S3Score score = S3Score.fromString(EXAMPLE_S3SCORE);
		
		Assert.assertEquals(exampleS3Score(), score);
	}
	
	@Test
	public void checkCyclicDeserializationOfS3Score() {
		String example = EXAMPLE_S3SCORE;
		
		for(int i=0; i<10; i++) {
			S3Score score = S3Score.fromString(example);
			example = score.toString();

			Assert.assertEquals(exampleS3Score(), score);
			Assert.assertEquals(EXAMPLE_S3SCORE, score.toString());
		}
	}
	
	private static S3Score exampleS3Score() {
		S3Score score = new S3Score();
		score.setChunksInA(0.1);
		score.setChunksInA(0.2);
		score.setCommonNGramms(3);
		score.setIdPair(Pair.of("left", "right"));
		score.setMeanChunks(0.5);
		score.setS3Score(0.6);
		
		return score;
	}
}
