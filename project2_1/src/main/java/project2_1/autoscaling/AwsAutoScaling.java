package project2_1.autoscaling;

import java.util.ArrayList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.Tag;

import project2_1.aws.AwsSecurityGroup;

public class AwsAutoScaling {
	
	public static AWSCredentials SetUpCredentials() {
		System.out.println("Set up credentials");
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
		                    "Cannot load the credentials from the credential profiles file. " +
		                    "Please make sure that your credentials file is at the correct " +
		                    "location (/home/qichen/.aws/credentials), and is in valid format.",
		                    e);
		}
		System.out.println("Done Setting up credentials");
		return credentials;
	}
	
	public static final String LOAD_GENERATOR_DNS = ""; // Hard coded lg dns
	public static final String AVALIBILITY_ZONE = "us-east-1d";

	public static void main(String[] args) {
		System.out.println("Start autoscaling");
		AWSCredentials credentials = SetUpCredentials();
		String groupId = AwsSecurityGroup.CreateSecurityGroup(AwsSecurityGroup.SECURITY_GROUP_NAME1, "First security Group for junor test");
		AwsSecurityGroup.CreateSecurityGroup(AwsSecurityGroup.SECURITY_GROUP_NAME2, "Second security Group for junor test");
		String elbDns = ElbAgent.SetUpELB(credentials, LOAD_GENERATOR_DNS, groupId);
		AutoScalingAgent.SetUpAutoScaling(credentials);
		System.out.println("Done autoscaling");
	}
}