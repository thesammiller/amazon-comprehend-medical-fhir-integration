package com.amazonaws.lab;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class FHIRAPIClient implements RequestHandler<String, String>{
	static final Logger log = LogManager.getLogger(FHIRAPIClient.class);
	static final String MEDIA_TYPE_FHIR_JSON = "application/fhir+json";
	//private static final String FHIR_API_ENDPOINT = System.getenv("FHIR_API_ENDPOINT");
	private static final String FHIR_API_ENDPOINT = "https://j1ws7u6iyl.execute-api.us-west-2.amazonaws.com/Prod/";

    @Override
    public String handleRequest(String input, Context context) {
    	Client client = ClientBuilder.newClient();
    	WebTarget target = client.target(FHIR_API_ENDPOINT);
    	WebTarget resourceWebTarget = target.path("metadata");
    	
    	Form form = new Form();
    	//get the Id token from Cognito API
    	String id_token = "eyJraWQiOiIwMjVRd3QrNTZYeFNObjE4Smk2azI5U3pKQSs3OTh6ekZMcUJWTHVxZ2RFPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiIxOWU3M2FmZi0zZWUzLTRmOTctYjEyNy00ZTQwNzljYTNhMDQiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLXdlc3QtMi5hbWF6b25hd3MuY29tXC91cy13ZXN0LTJfeVFMUVFqUzNkIiwiY29nbml0bzp1c2VybmFtZSI6Im1pbGxlcnMiLCJvcmlnaW5fanRpIjoiYjc4ZGVjMDgtOWJhNy00OGY1LTk3MWItNjUyZmY1MGQ0NjJlIiwiYXVkIjoiN2k2aXZzbmkxazN1MmcxZGtuZDg0anU3ODIiLCJldmVudF9pZCI6IjU4YTY4MWI4LWI0MGYtNDU4NS04OTA3LWVlZDM1NmY3ZWNlNSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjY0NjM2NjEwLCJleHAiOjE2NjQ2NDAyMTAsImlhdCI6MTY2NDYzNjYxMCwianRpIjoiMTNhMDhlNWUtMmFiMC00OTljLTgwMTEtZmNiZWYxYmQ1ZTE1IiwiZW1haWwiOiJzYW11ZWxzbWlsbGVyQGdtYWlsLmNvbSJ9.JCGNbmXKfd01sgESo0P_SHF-ReCwDHiZYnYfJzvv2-YwujkELBhLsR7Y4vzUfA_eD7rWhNOXsJ7P6caPJldkM2oTboWGfsXWv80K6cacHQhn5Tt8FyyJCCTa4DfxkIKTL73V7C9RHCt8m6hcgf83hyY3q2mOuV3mJ8Vo491LfB2DJV_JSYk4ztcRN4xBvCErB1hrUsM354P5xhwnQSUwAX4sm0EIdU21Sj2hOgSm8d2tAPnOxN02jEhIPpo4VSGKOP5b65Smj7kAO41MPXVJSpBy3rNjJClTYFd-DmTof51F-o6MLBl3T_PfxmWjRvQagENmEmDenWgM41JVGSrewg";
    	form.param("Authorization", id_token);
    	 
    	Invocation.Builder invocationBuilder =
    			resourceWebTarget.request("application/fhir+json");
    	
    	//invocationBuilder.header("Content-Type", "application/fhir+json");
    	//invocationBuilder.accept("application/json");
    	
    	Response response = invocationBuilder.get();
    	System.out.println("The response data : \n"+response.readEntity(String.class));
    	
    	//log.debug("The response data : \n"+response.readEntity(String.class));
    
    	
    	//target.request(MEDIA_TYPE_FHIR_JSON)
    	//    .post(Entity.entity(form,MediaType.APPLICATION_FORM_URLENCODED_TYPE),
    	//        MyJAXBBean.class);
    	return null;
    }
    public static void main(String []args) {
    	FHIRAPIClient client = new FHIRAPIClient();
    	client.handleRequest("test", null);
    }
    
    

}
