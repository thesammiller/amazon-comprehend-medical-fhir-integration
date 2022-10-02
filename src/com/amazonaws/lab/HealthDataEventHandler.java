package com.amazonaws.lab;

import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.io.FilenameUtils;

//import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;


import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

//import software.amazon.awssdk.services.sfn;
//import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;



public class HealthDataEventHandler implements RequestHandler<SQSEvent, Void>{
	
	private ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
	static final Logger log = LogManager.getLogger(HealthDataEventHandler.class);
	private static final String STATE_MACHINE_ARN = System.getenv("STATE_MACHINE_ARN");
    /**
     * This handler is triggered by an SQS-put-object event, and triggers a step function
     */
    @Override
    public Void handleRequest(SQSEvent input, Context context) {
    	List<SQSMessage> records = input.getRecords();
    	log.debug("The records received size : "+records.size());
    	String sKey = "";
    	String sBucket = "";
    	String sFileName = "";
    	String sUUID = "";
		for(SQSMessage msg : records) {
			String body = msg.getBody();
			log.debug("The message body : "+ msg.getBody());
			
			S3EventNotification s3EventNotf = S3EventNotification.parseJson(body);
			log.debug("The S3 notification body : "+ s3EventNotf.getRecords());
			List<S3EventNotificationRecord> s3EventRecords = s3EventNotf.getRecords();
			for(S3EventNotificationRecord s3EventRec : s3EventRecords) {
				sKey = s3EventRec.getS3().getObject().getKey();
				sBucket = s3EventRec.getS3().getBucket().getName();
				sUUID = UUID.randomUUID().toString();
				sFileName = FilenameUtils.getName(sKey) + "-" + sUUID;
			}
		}
		
		// Write object to /processing/<s3Key>
		//final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
		
		Region region = Region.US_WEST_2;
		ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();

		S3Client s3 = S3Client.builder()
									.region(region)
									.credentialsProvider(credentialsProvider)
									.build();
		
		try {
		    //s3.copyObject(sBucket, sKey, sBucket, newFile);
		    String newFile = "processing/" + sKey + "-" + sUUID;
		    CopyObjectRequest copyObject = CopyObjectRequest.builder()
		    												.sourceBucket(sBucket)
		    												.sourceKey(sKey)
		    												.destinationBucket(sBucket)
		    												.destinationKey(newFile)
		    												.build();
		    s3.copyObject(copyObject);
		    
		    DeleteObjectRequest deleteObject = DeleteObjectRequest.builder()
		    														.bucket(sBucket)
		    														.key(sKey)
		    														.build();
		    
		    //s3.deleteObject(sBucket, sKey);
		    s3.deleteObject(deleteObject);
		    
		} catch (S3Exception e) {
		    log.debug("S3 Exception");
		}
		
		
		String sfnInput = "{\"S3Bucket\":\"" + sBucket +"\", \"FileName\":\"" + sFileName +"\", \"InputFile\":\"" + "processing/" + sKey + "-" + sUUID +"\"}";
		log.debug("Step functions input is : "+sfnInput);
		AWSStepFunctions sfnClient = AWSStepFunctionsClient.builder().build();
		//SfnClient sfnClient = SfnClient.builder().credentialsProvider(credentialsProvider).build();
		StartExecutionRequest excReq = new StartExecutionRequest();
		excReq.setStateMachineArn(STATE_MACHINE_ARN);
		excReq.setInput(sfnInput);
		
		/*
		StartExecutionRequest excReq = StartExecutionREquest.builder()
															.stateMachineArn(STATE_MACHINE_ARN)
															.input(sfnInput)
															.build();
															
		*/
		sfnClient.startExecution(excReq);
		
        return null;
    }
    
}
