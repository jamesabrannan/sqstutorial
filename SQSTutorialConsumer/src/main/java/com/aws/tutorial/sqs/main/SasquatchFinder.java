package com.aws.tutorial.sqs.main;

import java.util.List;
import java.util.Map;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SasquatchFinder {

	private SqsClient sqsClient;
	private String queueUrl;
	public static int finderId = 1;

	public SasquatchFinder(String key, String secretKey, String queueUrl) {
		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(key, secretKey);
		this.sqsClient = SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(awsCreds))
				.region(Region.US_EAST_1).build();
		this.queueUrl = queueUrl;
	}

	public void processMessage() {

		ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(this.queueUrl)
				.maxNumberOfMessages(10).messageAttributeNames("*").build();
		List<Message> messages = this.sqsClient.receiveMessage(receiveMessageRequest).messages();
		if (messages == null || messages.size() == 0) {
			return;
		}
		messages.stream().map(s -> s.body()).forEach(System.out::println);

		for (Message message : messages) {
			System.out.println(message.messageId());
			Map<String, MessageAttributeValue> attributes = message.messageAttributes();
			Set<String> keys = attributes.keySet();
			
			for (String key : keys) {
				System.out.println(key + ":" + attributes.get(key).stringValue());
			}
		}
		try {
			System.out.println("sleeping for 10 seconds...");
			Thread.sleep(10000);
			this.deleteMessages(messages);
			//this.deleteMessage(messages);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void deleteMessages(List<Message> messages) {
		for(Message message:messages) {
			String receiptHandle = message.receiptHandle();
			DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder().queueUrl(this.queueUrl)
					.receiptHandle(receiptHandle).build();
			this.sqsClient.deleteMessage(deleteRequest);
			System.out.println("Deleted message " + receiptHandle + " by SasquatchFinder " + SasquatchFinder.finderId);
		}
	}
	
	public void deleteMessage(List<Message> messages) {
		String receiptHandle = messages.get(0).receiptHandle();
		DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder().queueUrl(this.queueUrl)
				.receiptHandle(receiptHandle).build();
		this.sqsClient.deleteMessage(deleteRequest);
		System.out.println("Deleted message " + receiptHandle + " by SasquatchFinder " + SasquatchFinder.finderId);
	}

	public static void main(String[] args) {
		SasquatchFinder.finderId = Integer.parseInt(args[0]);
		System.out.println("SasquatchFinder " + SasquatchFinder.finderId + " running....");
		SasquatchFinder finder = new SasquatchFinder(args[1], args[2],
				args[3]);
		try {
			while (true) {
				finder.processMessage();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("SasquatchFinder " + SasquatchFinder.finderId + " stopped.");
	}
}
