package com.javaKinesis.org;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class CreateStream {

	private static String endpoint = "kinesis.us-east-2.amazonaws.com";
	private static AWSCredentialsProvider credentialsProvider;
	private static String myStreamName = "Stream_Created_Using_Java1";

	private static void init() {
		// Ensure the JVM will refresh the cached IP values of AWS resources
		// (e.g. service endpoints).
		java.security.Security.setProperty("networkaddress.cache.ttl", "60");

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (C:\\Users\\miths_000\\.aws\\credentials).
		 */
		credentialsProvider = new ProfileCredentialsProvider();
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (C:\\Users\\miths_000\\.aws\\credentials), and is in valid format.",
					e);
		}
	}

	public static void main(String[] args) {

		init();

		// create client object and set endpoint
		AmazonKinesisClient client = new AmazonKinesisClient();
		client.setEndpoint(endpoint);

		// create stream object
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(myStreamName);
		createStreamRequest.setShardCount(3);

		client.createStream(createStreamRequest);
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(myStreamName);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (startTime < endTime) {
			try {
				Thread.sleep(20 * 1000);
			} catch (Exception e) {
			}

			try {
				DescribeStreamResult describeStreamResponse = client
						.describeStream(describeStreamRequest);
				String streamStatus = describeStreamResponse
						.getStreamDescription().getStreamStatus();
				if (streamStatus.equals("ACTIVE")) {
					System.out.println("Stream is now Active");
					break;
				}
				//
				// sleep for one second
				//
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			} catch (ResourceNotFoundException e) {
			}
		}
		if (System.currentTimeMillis() >= endTime) {
			throw new RuntimeException("Stream " + myStreamName
					+ " never went active");
		}

	}
}
