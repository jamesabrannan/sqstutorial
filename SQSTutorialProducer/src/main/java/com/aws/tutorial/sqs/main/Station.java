package com.aws.tutorial.sqs.main;

import javax.annotation.PreDestroy;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class Station {
	
	SqsClient sqsClient;
	String queueUrl;
	
	public Station(String key, String secretKey, String queueUrl) {
		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(key, secretKey);
		this.sqsClient = SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(awsCreds)).region(Region.US_EAST_1).build();
		this.queueUrl = queueUrl;
	}
	
	public String sendMessage(String message) {
		SendMessageRequest request = SendMessageRequest.builder().queueUrl(this.queueUrl).messageBody(message)
				//.messageGroupId("mygroup").delaySeconds(5).build();	
				.messageGroupId("mygroup").build();	
		SendMessageResponse response = this.sqsClient.sendMessage(request);
		return response.messageId();
	}

	@PreDestroy
	public void preDestroy() {
		this.sqsClient.close();
	}
	
	
	public static void main(String[] args) {
		System.out.println("Station running....");
		Station station = new Station(args[0],args[1],
				args[2]);
		String id = station.sendMessage(TestData.observationOne);
		System.out.println("sent message: " + id);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		id = station.sendMessage(TestData.observationTwo);
		System.out.println("sent message: " + id);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		id = station.sendMessage(TestData.observationThree);
		System.out.println("sent message: " + id);
	}
}
