package com.amazonaws.lab;

import java.util.Map;
import java.util.HashMap;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.amazonaws.services.lambda.runtime.Context;
//import com.amazonaws.services.comprehendmedical.AWSComprehendMedical;
//import com.amazonaws.services.comprehendmedical.AWSComprehendMedicalClientBuilder;
//import com.amazonaws.services.comprehendmedical.model.DetectEntitiesRequest;
//import com.amazonaws.services.comprehendmedical.model.DetectEntitiesResult;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;


import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.comprehendmedical.ComprehendMedicalClient;
import software.amazon.awssdk.services.comprehendmedical.model.DetectEntitiesRequest;
import software.amazon.awssdk.services.comprehendmedical.model.DetectEntitiesResponse;
import software.amazon.awssdk.services.comprehendmedical.model.ComprehendMedicalException;



public class SendToCM {
    static final Logger log = LogManager.getLogger(SendToCM.class);
    
    //private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    private Region region = Region.US_WEST_2;
	private ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();


	private S3Client s3 = S3Client.builder()
									.region(region)
									.credentialsProvider(credentialsProvider)
									.build();

    
    public Map<String, String> handleRequest(Map<String, String> map, Context context) {
        // Get UnstructuredText as string from S3 object
        //String UnstructuredText = s3Client.getObjectAsString(map.get("S3Bucket"), map.get("UnstructuredText"));
        String s3Bucket = map.get("S3Bucket");
        String fileKey = map.get("UnstructuredText");
        
        GetObjectRequest objectRequest = GetObjectRequest.builder()
															 .key(fileKey)
															 .bucket(s3Bucket)
															 .build();
															 

		ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
		byte[] data = objectBytes.asByteArray();
		String UnstructuredText = new String(data);
        
        
        // Parse "UnstucturedText" to get just the notes field
        JsonObject obj = new Gson().fromJson(UnstructuredText, JsonObject.class);
        String notes = obj.get("notes").toString();
        
        // Send "notes" string to Comprehend Medical
        //final AWSComprehendMedical client = AWSComprehendMedicalClientBuilder.defaultClient();
        
        ComprehendMedicalClient medClient = ComprehendMedicalClient.builder()
                                                                    .region(region)
                                                                    .build();
        
        //DetectEntitiesRequest request = new DetectEntitiesRequest();
        //request.setText(notes);
        
        DetectEntitiesRequest detectEntitiesRequest = DetectEntitiesRequest.builder()
                                                                            .text(notes)
                                                                            .build();
        
        
        String resultOutput = "";
        //DetectEntitiesResult result = client.detectEntities(request);
        DetectEntitiesResponse detectEntitesResponse = medClient.detectEntities(detectEntitiesRequest);
        
        
        
        //mithun : Made a change to store a well formed json. The default to toString doesnt have the quotes
        //resultOutput = result.getEntities().toString();
        resultOutput = new Gson().toJson(detectEntitesResponse.entities());
        
        //s3Client.putObject(map.get("S3Bucket"), "processing/CMOutput/" + map.get("FileName"), resultOutput);
        
        String bucketName = map.get("S3Bucket");
        String key = "processing/CMOutput/" + map.get("FileName");
        PutObjectRequest putOb = PutObjectRequest.builder()
													 .bucket(bucketName)
													 .key(key)
													 .build();
			
			
			Charset charset = Charset.forName("ASCII");
			
			ByteBuffer buffer = ByteBuffer.wrap(resultOutput.getBytes(charset));
			PutObjectResponse response = s3.putObject(putOb, RequestBody.fromByteBuffer(buffer));
        
        
        Map<String, String> output = new HashMap<>();
        output.put("S3Bucket", map.get("S3Bucket"));
        output.put("CMOutput", "processing/CMOutput/" + map.get("FileName"));
        output.put("UnstructuredText", map.get("UnstructuredText"));
        output.put("FileName", map.get("FileName"));
        output.put("InputFile", map.get("InputFile"));
        output.put("DataType", map.get("DataType"));

        return output;
    }
}
