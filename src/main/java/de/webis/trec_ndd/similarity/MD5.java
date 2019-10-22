package de.webis.trec_ndd.similarity;

import com.google.common.base.Charsets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class MD5 {

	@SuppressWarnings("deprecation")
	public static String md5hash(String input) {
		StringBuilder hashStringBuilder=new StringBuilder();
		Hasher hasher = Hashing.md5().newHasher();
		hasher.putString(input, Charsets.UTF_8);
		byte[] hash = hasher.hash().asBytes();
		for (byte a : hash) hashStringBuilder.append(String.format("%02X",a));
		return hashStringBuilder.toString().toLowerCase();
	}

}
