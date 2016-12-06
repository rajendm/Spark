package com.javaKinesis.org;

import java.lang.reflect.Method;
import java.util.List;
import java.util.ListIterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

public class ListingStreams {

	private static String endpoint = "kinesis.us-east-2.amazonaws.com";

	public void listStreams() {
		AmazonKinesisClient client = new AmazonKinesisClient();
		client.setEndpoint(endpoint);

		// Listing Streams
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(20);
		ListStreamsResult listStreamsResult = client
				.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();

		// Printing Listed Streams

		while (listStreamsResult.getHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames
						.get(streamNames.size() - 1));
			}
			listStreamsResult = client.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());
		}

		ListIterator<String> it = streamNames.listIterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) {

		try {

			Method m = CreateStream.class.getDeclaredMethod("init", null);
			m.setAccessible(true);
			Object o = m.invoke(null);
			ListingStreams lt = new ListingStreams();
			lt.listStreams();

		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}
