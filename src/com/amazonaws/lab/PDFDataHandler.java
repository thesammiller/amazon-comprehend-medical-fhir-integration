package com.amazonaws.lab;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.Gson;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.model.S3Object;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.Document;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextRequest;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextResponse;
import software.amazon.awssdk.services.textract.model.Block;
import software.amazon.awssdk.services.textract.model.DocumentMetadata;
import software.amazon.awssdk.services.textract.model.TextractException;
import java.util.Iterator;
import java.util.List;

public class PDFDataHandler {
	static final Logger log = LogManager.getLogger(PDFDataHandler.class);

	private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
	private static String timeZoneId = System.getenv("FHIR_TIME_ZONE");

	/**
	 * This method is called by the step functions
	 * 
	 * @param input
	 * @param context
	 * @return
	 */

	public Map<String, String> handleRequest(Map<String, String> map, Context context) {
		
		//https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/javav2/example_code/textract/src/main/java/com/example/textract/DetectDocumentTextS3.java
        String bucketName = map.get("S3Bucket");
        String docName = map.get("InputFile");
		log.debug("PDFinput: " + docName);

		String textString = "";
		try {
			Region region = Region.US_WEST_2;
			System.out.println("Region set to us-west-2");
			
			 TextractClient textractClient = TextractClient.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
			
			System.out.println("Textract client");
			S3Object s3Object = S3Object.builder()
                .bucket(bucketName)
                .name(docName)
                .build();
            System.out.println("S3Object client");
            // Create a Document object and reference the s3Object instance
            Document myDoc = Document.builder()
                .s3Object(s3Object)
                .build();
            System.out.println("Document build");
            DetectDocumentTextRequest detectDocumentTextRequest = DetectDocumentTextRequest.builder()
                .document(myDoc)
                .build();
            System.out.println("Detect Doc Text");
            
            DetectDocumentTextResponse textResponse = textractClient.detectDocumentText(detectDocumentTextRequest);
            
            System.out.println("Hello");
            
            for (Block block: textResponse.blocks()) {
            	System.out.println(block.text());
            	textString = textString + block.text();
                System.out.println("The block type is " +block.blockType().toString());
            }

            DocumentMetadata documentMetadata = textResponse.documentMetadata();
            System.out.println("The number of pages in the document is " +documentMetadata.pages());

			
		} catch (Exception e) {
			System.out.println("Dude, exception");
			System.out.println(e.getMessage());
           
        }

		System.out.println("Output S3 path: " + map.get("S3Bucket") + "processing/unstructuredtext/" + map.get("FileName"));
		putS3ObjectContentAsString(map.get("S3Bucket"), "processing/unstructuredtext/" + map.get("FileName"),
				textString);

		// Create our output response back to the state machine
		Map<String, String> output = new HashMap<>();
		output.put("S3Bucket", map.get("S3Bucket"));
		output.put("FileName", map.get("FileName"));
		output.put("InputFile", map.get("InputFile"));
		output.put("DataType", map.get("DataType"));
		output.put("UnstructuredText", "processing/unstructuredtext/" + map.get("FileName"));

		return output;
	}
	
	public String putS3ObjectContentAsString(String bucketName, String key, String content) {
		try {
			s3Client.putObject(bucketName, key, content);
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		return "Done";
	}
	
	public static void main(String[] args) {
	
		PDFDataHandler handler = new PDFDataHandler();
		Map<String, String> dataMap = new HashMap();
		
		String bucketKey = "S3Bucket";
		String docKey = "InputFile";
		String bucketValue = "fhir-cm-integ-healthinfobucket-1rlvfxyky58d0";
		String docValue = "/input/pdf/health_sample.pdf";
		
		dataMap.put(bucketKey, bucketValue);
		dataMap.put(docKey, docValue);
		dataMap.put("FileName", "health_sample.pdf");
		dataMap.put("DataType", "pdf");
		
		Map<String, String> response = handler.handleRequest(dataMap, null);
		
		System.out.println(response);
		
	}

}
