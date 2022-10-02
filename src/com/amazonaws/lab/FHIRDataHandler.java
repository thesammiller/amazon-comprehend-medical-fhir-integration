package com.amazonaws.lab;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.dstu3.model.DocumentReference;
import org.hl7.fhir.dstu3.model.DocumentReference.DocumentReferenceContentComponent;
import org.hl7.fhir.dstu3.model.Enumerations.ResourceType;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Resource;

import software.amazon.awssdk.core.ResponseBytes;

//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

import software.amazon.awssdk.core.sync.RequestBody;


import com.google.gson.Gson;

import ca.uhn.fhir.context.FhirContext;

public class FHIRDataHandler {
	static final Logger log = LogManager.getLogger(FHIRDataHandler.class);

	//private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
	private Region region = Region.US_WEST_2;
	private ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();


	private S3Client s3 = S3Client.builder()
									.region(region)
									.credentialsProvider(credentialsProvider)
									.build();

	
	private static FhirContext fhirContext = FhirContext.forDstu3();
	
	public Map<String, String> handleRequest(Map<String, String> map, Context context) {
		// check file type - HL7 or FHIR
		String fileType = map.get("DataType");
		
		String s3Bucket = map.get("S3Bucket");
		
		String fileKey = map.get("InputFile");
		// Get S3 object as string, then process it
		//String fhirInput = s3Client.getObjectAsString(s3Bucket, fileKey);
		
		GetObjectRequest objectRequest = GetObjectRequest.builder()
															 .key(fileKey)
															 .bucket(s3Bucket)
															 .build();
															 

		ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
		byte[] data = objectBytes.asByteArray();
		String fhirInput = new String(data);
			
			
		DocumentReference docRef = fhirContext.newJsonParser().parseResource(DocumentReference.class, fhirInput);
		String patientRef = docRef.getSubject().getReference();
		
		ArrayList<String> notesList = this.getNotesFromDocument(docRef);
		
		PatientInfo patInfo = new PatientInfo();
		patInfo.setPatientId(patientRef);

	
		patInfo.setNotes(notesList);
		
		String json = new Gson().toJson(patInfo);
		
		putS3ObjectContentAsString(map.get("S3Bucket"), "processing/unstructuredtext/" + map.get("FileName"),
				json.toString());
		
		// Create our output response back to the state machine
		Map<String, String> output = new HashMap<>();
		output.put("S3Bucket", map.get("S3Bucket"));
		output.put("FileName", map.get("FileName"));
		output.put("InputFile", map.get("InputFile"));
		output.put("DataType", map.get("DataType"));
		output.put("UnstructuredText", "processing/unstructuredtext/" + map.get("FileName"));
		return output;
	}
	private Patient getPatientResourceFromBundle(Bundle bundle) {
		List<BundleEntryComponent> bundCompList = bundle.getEntry();
		Patient pat = null;
		for (BundleEntryComponent comp : bundCompList) {
			Resource res = comp.getResource();

			if (res.fhirType() == ResourceType.PATIENT.getDisplay()) {
				pat = (Patient) res;
				break;
			}
		}
		return pat;
	}
	
	public String putS3ObjectContentAsString(String bucketName, String key, String content) {
		try {
			// TODO: V2 Update here
			//s3Client.putObject(bucketName, key, content);
			PutObjectRequest putOb = PutObjectRequest.builder()
													 .bucket(bucketName)
													 .key(key)
													 .build();
			
			
			Charset charset = Charset.forName("ASCII");
			
			ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(charset));
			PutObjectResponse response = s3.putObject(putOb, RequestBody.fromByteBuffer(buffer));
			
		} catch (S3Exception e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		}
		return "Done";
	}
	
	private ArrayList<String> getNotesFromBundle(Bundle bundle) {
		ArrayList<String> notesList = new ArrayList<String>();
		List<BundleEntryComponent> list = bundle.getEntry();

		for(BundleEntryComponent entry : list) {
			String fhirType = entry.getResource().fhirType();
			
			if(fhirType.equals(ResourceType.DOCUMENTREFERENCE.getDisplay())) {
				DocumentReference docRef = (DocumentReference)entry.getResource();
				log.debug("The document data "+docRef.getDescription());
				List<DocumentReferenceContentComponent> attList = docRef.getContent();
				StringBuffer buffer = new StringBuffer();
				for(DocumentReferenceContentComponent attach:attList) {
					byte[] notesBytes = attach.getAttachment().getData();
					log.debug("The provider clinical :"+new String(notesBytes));
					buffer.append(new String(notesBytes));
				}
				
				notesList.add(buffer.toString());
			}
		}
		return notesList;
		
	}
	
	private ArrayList<String> getNotesFromDocument(DocumentReference docRef) {
		ArrayList<String> notesList = new ArrayList<String>();
			
		log.debug("The document data "+docRef.getDescription());
		List<DocumentReferenceContentComponent> attList = docRef.getContent();
		StringBuffer buffer = new StringBuffer();
		for(DocumentReferenceContentComponent attach:attList) {
			byte[] notesBytes = attach.getAttachment().getData();
			log.debug("The provider clinical :"+new String(notesBytes));
			buffer.append(new String(notesBytes));
		}
		
		notesList.add(buffer.toString());
		return notesList;
		
	}
}
