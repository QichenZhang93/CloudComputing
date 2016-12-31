package project2_1.autoscaling;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckResult;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.Tag;

import project2_1.aws.AwsSecurityGroup;

public class ElbAgent {
	
	public static final String LOAD_BANLANCER_NAME = "15619project21";
	
	private static void setUpHeathCheck(AmazonElasticLoadBalancingClient elbCheck, String lgDns) {
		// Health Check
				ConfigureHealthCheckRequest healthCheckRequest = new ConfigureHealthCheckRequest();
				HealthCheck healthCheck = new HealthCheck();
				healthCheck.withUnhealthyThreshold(2).withInterval(30).withTimeout(10).withHealthyThreshold(2)
					.withTarget("HTTP:80/heartbeat?lg=" + lgDns);
				healthCheckRequest.withHealthCheck(healthCheck).withLoadBalancerName(LOAD_BANLANCER_NAME);
				ConfigureHealthCheckResult cHealthCheckResult = elbCheck.configureHealthCheck(healthCheckRequest);
	}

	public static String SetUpELB(AWSCredentials credentials, String LGDns, String groupId) {
		System.out.println("Set up ELB on LG: " + LGDns);
		AmazonElasticLoadBalancingClient ElbClient = new AmazonElasticLoadBalancingClient(credentials);
		CreateLoadBalancerRequest cRequest = new CreateLoadBalancerRequest();
		Tag tag = new Tag();
		tag.setKey("Project");
		tag.setValue("2.1");
		cRequest.withLoadBalancerName(LOAD_BANLANCER_NAME)
			.withSecurityGroups(groupId)
			.withTags(tag)
			.withAvailabilityZones(AwsAutoScaling.AVALIBILITY_ZONE)
			.withListeners(new Listener("HTTP", 80, 80))
			.withScheme("internet-facing");
		CreateLoadBalancerResult result = ElbClient.createLoadBalancer(cRequest);
		String elbDns = result.getDNSName();
		
		setUpHeathCheck(ElbClient, LGDns);
		
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Done setting up credentials");
		return elbDns;
	}
}
