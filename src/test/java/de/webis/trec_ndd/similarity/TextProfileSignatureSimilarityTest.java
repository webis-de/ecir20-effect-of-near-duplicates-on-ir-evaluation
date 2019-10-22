package de.webis.trec_ndd.similarity;

import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.similarity.TextProfileSignatureSimilarity;
import de.webis.trec_ndd.similarity.TextSimilarity;

public class TextProfileSignatureSimilarityTest {
	private static final String EXAMPLE_A = "Hello World ac ad Test";
	
	private static final String EXAMPLE_B = "ga de World ac ad Test ds ds Hello";
	
	private static final String EXAMPLE_C = "ga de World ac ad ds ds Hello";

	private static final String EXAMPLE_D = "World Hello ga de world hello";
	
	@Test
	public void sampleTextShouldBeSimilarToItself() {
		TextSimilarity sim = new TextProfileSignatureSimilarity();
		String text = "Hallo Welt";
		
		Assert.assertTrue(sim.textsAreSimilar(text, text));
	}
	
	@Test
	public void disjointTextsShouldBeUnsimilar() {
		TextSimilarity sim = new TextProfileSignatureSimilarity();
		
		Assert.assertFalse(sim.textsAreSimilar("Hallo Welt", "Hello World"));
	}
	
	@Test
	public void exampleTextAShouldBeSimilarToB() {
		TextSimilarity sim = new TextProfileSignatureSimilarity();
		
		Assert.assertTrue(sim.textsAreSimilar(EXAMPLE_A, EXAMPLE_B));
	}
	
	@Test
	public void exampleTextAShouldNotBeSimilarToC() {
		TextSimilarity sim = new TextProfileSignatureSimilarity();
		
		Assert.assertFalse(sim.textsAreSimilar(EXAMPLE_A, EXAMPLE_C));
	}
	
	@Test
	public void exampleTextCShouldBeSimilarToD() {
		TextSimilarity sim = new TextProfileSignatureSimilarity();
		
		Assert.assertTrue(sim.textsAreSimilar(EXAMPLE_C + " Hello world ", EXAMPLE_D + " WoRld helLo "));
	}
}
