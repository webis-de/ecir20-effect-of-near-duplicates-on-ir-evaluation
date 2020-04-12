package de.webis.trec_ndd.util;

import java.io.InputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.Data;

@Data
@SuppressWarnings("serial")
public class S3Files implements Serializable {
	private final String accessKey, secretKey, bucketName;

	public List<S3ObjectSummary> filesInBucket() {
		List<S3ObjectSummary> ret = new LinkedList<>();
		AmazonS3 s3Connection = s3();
		ObjectListing objects = s3Connection.listObjects(bucketName);

		do {
			for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
				ret.add(objectSummary);
			}
			objects = s3Connection.listNextBatchOfObjects(objects);
		} while (objects.isTruncated());

		return ret;
	}

	public InputStream rawContent(String key) {
		S3Object objectInS3 = s3().getObject(bucketName, key);

		return objectInS3.getObjectContent();
	}

	@SuppressWarnings("deprecation")
	private AmazonS3 s3() {
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

		ClientConfiguration config = new ClientConfiguration();
		config.setSignerOverride("S3SignerType");

		S3ClientOptions options = S3ClientOptions.builder().setPathStyleAccess(Boolean.TRUE).build();

		// I can not pass the options using the non-deprecated AmazonS3ClientBuilder.
		// Hence I use the deprecated variant
		AmazonS3Client ret = new AmazonS3Client(new AWSStaticCredentialsProvider(credentials), config);
		ret.setEndpoint("http://s3.dw.webis.de:7480");
		ret.setS3ClientOptions(options);

		return ret;
	}
}

