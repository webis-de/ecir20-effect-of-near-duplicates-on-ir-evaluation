package de.webis.trec_ndd.util;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;

//import org.apache.commons.exec.ExecuteException;
import org.netspeak.client.Netspeak;
import org.netspeak.client.Request;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

@UtilityClass
public class WordCounts {
	private static final String NETSPEAK_API = "https://api.netspeak.org/netspeak3/search?";
	
	public static RetryPolicy<Long> retryPolicy = new RetryPolicy<Long>()
			  .handle(Exception.class)
			  .withBackoff(5, 180, ChronoUnit.SECONDS)
			  .withMaxRetries(3);
	
	private static final LoadingCache<String, Long> WORD_COUNT = CacheBuilder.newBuilder()
			.build(CacheLoader.from(WordCounts::wordCountFailsave));

	@SneakyThrows
	public static Long getWordCount(String word) throws ExecutionException{
		return WORD_COUNT.get(word);
	}
	
	private static Long wordCountFailsave(String word) {
		return Failsafe.with(retryPolicy)
				.get(() -> getWordCountOrFail(word));
	}
	
	private static long getWordCountOrFail(String word) throws IOException {
		Request request = new Request();
		request.put(Request.QUERY, word);
		
		return new Netspeak(NETSPEAK_API)
				.search(request).getTotalFrequency();
	}
}
