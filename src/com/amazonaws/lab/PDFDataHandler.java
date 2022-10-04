package com.amazonaws.lab;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.google.gson.Gson;

//import com.amazonaws.services.s3.model.S3ObjectINputStream;
//import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

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
    //private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    private static String timeZoneId = System.getenv("FHIR_TIME_ZONE");

    private Region region = Region.US_WEST_2;
	private ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();


	private S3Client s3 = S3Client.builder()
									.region(region)
									.credentialsProvider(credentialsProvider)
									.build();    
    /* This method is called by the step functions
    
    */
    public Map<String, String> handleRequest(Map<String, String> map, Context context) {
        String bucketName = map.get("S3Bucket");
        String docName = map.get("InputFile");
        log.debug("PDFinput: " + docName);
        
        String textString = "";
        try {
            //https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/javav2/example_code/textract
            TextractClient textractClient = TextractClient.builder()
                                                            .region(region)
                                                            .credentialsProvider(credentialsProvider)
                                                            .build(); 
        
            String docText = detectDocTextS3(textractClient, bucketName, docName);
            
            
            //See HL7DataHandler
        	putS3ObjectContentAsString(bucketName, "processing/unstructuredtext/" + map.get("FileName"), docText);
            
            // Create our output response back to the state machine
    		Map<String, String> output = new HashMap<>();
    		output.put("S3Bucket", map.get("S3Bucket"));
    		output.put("FileName", map.get("FileName"));
    		output.put("InputFile", map.get("InputFile"));
    		output.put("DataType", map.get("DataType"));
    		output.put("UnstructuredText", "processing/unstructuredtext/" + map.get("FileName"));

    		return output;
            
        } finally {
            log.debug("Done");
        }
    }
    
    
    public String detectDocTextS3 (TextractClient textractClient, String bucketName, String docName) {
        String blockText = "";
        try {
    
                
                S3Object s3Object = S3Object.builder()
                                            .bucket(bucketName)
                                            .name(docName)
                                            .build();
                 // Create a Document object and reference the s3Object instance
                Document myDoc = Document.builder()
                    .s3Object(s3Object)
                    .build();
                
                DetectDocumentTextRequest detectDocumentTextRequest = DetectDocumentTextRequest.builder()
                                                                                            .document(myDoc)
                                                                                            .build();
    
                DetectDocumentTextResponse textResponse = textractClient.detectDocumentText(detectDocumentTextRequest);
                
                 for (Block block: textResponse.blocks()) {
                    blockText = blockText + " " + block.text();
                }
            } catch (Exception e) {
                log.debug("Error on PDF Detect Doc Text block");
            }
            
            return blockText;
        }
        
    public String putS3ObjectContentAsString(String bucketName, String key, String content) {
		try {
			//s3Client.putObject(bucketName, key, content);
			PutObjectRequest putOb = PutObjectRequest.builder()
													 .bucket(bucketName)
													 .key(key)
													 .build();
			
			
			Charset charset = Charset.forName("ASCII");
			
			ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(charset));
			PutObjectResponse response = s3.putObject(putOb, RequestBody.fromByteBuffer(buffer));
			
		} catch (S3Exception e) {
			e.printStackTrace();
		}
		return "Done";
	}
    
}



