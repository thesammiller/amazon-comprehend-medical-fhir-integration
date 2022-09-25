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
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.BoundingBox;
import com.amazonaws.services.textract.model.DetectDocumentTextRequest;
import com.amazonaws.services.textract.model.DetectDocumentTextResult;
import com.amazonaws.services.textract.model.Document;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.Point;
import com.amazonaws.services.textract.model.Relationship;
import com.amazonaws.services.textract.model.AmazonTextractException;
import com.amazonaws.services.textract.AmazonTextractClient;



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
			
			
			//https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/java/example_code/textract/src/main/java/com/amazonaws/samples/DocumentText.java
			//to translate the Python approach
			//https://github.com/aws-samples/amazon-textract-and-comprehend-medical-document-processing
			
			//"In the next step, we will start the asynchronous textract operation by calling the start_document_analysis function.
			//the function will kickoff an asynchronous job that will process our medical report file in the stipulated S3 bucket"
			
			Document document = new Document().withS3Object(new S3Object().withName(docName).withBucket(bucketName));
			// Call DetectDocumentText
	        EndpointConfiguration endpoint = new EndpointConfiguration(
	                "https://textract.us-west-2.amazonaws.com", "us-west-2");
	        AmazonTextract client = AmazonTextractClientBuilder.standard()
	                				.withEndpointConfiguration(endpoint).build();
			//Detect Text in the Document
			 DetectDocumentTextRequest request = new DetectDocumentTextRequest()
	        											 .withDocument(document);

			DetectDocumentTextResult result = client.detectDocumentText(request);
            
            for (Block block: result.getBlocks()) {
            	if (block.getBlockType() == "LINE") {
            		System.out.println("Hello");
            		//block.text()
            		//The word or line of text that's recognized by Amazon Textract.
            		textString = textString + block.getText();
	                System.out.println("The block text  is " + block.getText());
            	}
            }
        	
		} catch (AmazonTextractException e) {

            System.err.println(e.getMessage());
            System.exit(1);
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
		String bucketValue = "fhir-cm-integ-healthinfobucket-1x7y9a53ogmei";
		String docValue = "/input/pdf/health_sample.pdf";
		
		dataMap.put(bucketKey, bucketValue);
		dataMap.put(docKey, docValue);
		dataMap.put("Filename", "/input/pdf/health_sample.pdf");
		dataMap.put("DataType", "pdf");
		
		Map<String, String> response = handler.handleRequest(dataMap, null);
		
		System.out.println(response);
		
	}

}
