package project2_1.aws;

import java.util.Collections;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.IpPermission;

public class AwsSecurityGroup {
	
	public static String SECURITY_GROUP_NAME1 = "AwsAutoScaling1_15619project21";
	public static String SECURITY_GROUP_NAME2 = "AwsAutoScaling2_15619project21";
	
	public static String CreateSecurityGroup(String sGroupName, String sGroupDesc) {
		System.out.println("Create Security Group: " + sGroupName);
		/*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/home/qichen/.aws/credentials).
         */
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

        // Create the AmazonEC2Client object so we can call various APIs.
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        Region usEast = Region.getRegion(Regions.US_EAST_1);
        ec2.setRegion(usEast);
        
        String groupId = "";

        // Create a new security group.
        try {
            CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(
            		sGroupName, sGroupDesc);
            CreateSecurityGroupResult result = ec2
                    .createSecurityGroup(securityGroupRequest);
            groupId = result.getGroupId();
            System.out.println(String.format("Security group created: [%s]",
                    result.getGroupId()));
        } catch (AmazonServiceException ase) {
            // Likely this means that the group is already created, so ignore.
            System.out.println(ase.getMessage());
        }

        String ipAddr = "0.0.0.0/0";
        // Create a range that you would like to populate.

        // Open up port 23 for TCP traffic to the associated IP from above (e.g. ssh traffic).
        IpPermission ipPermission = new IpPermission()
                .withIpProtocol("-1")
                .withFromPort(new Integer(0))
                .withToPort(new Integer(65535))
                .withIpRanges(ipAddr);

        List<IpPermission> ipPermissions = Collections.singletonList(ipPermission);

        try {
            // Authorize the ports to the used.
            AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest(
            		sGroupName, ipPermissions);
            ec2.authorizeSecurityGroupIngress(ingressRequest);
            System.out.println(String.format("Ingress port authroized: [%s]",
                    ipPermissions.toString()));
        } catch (AmazonServiceException ase) {
            // Ignore because this likely means the zone has already been authorized.
            System.out.println(ase.getMessage());
        }
        
        System.out.println("Done creating Security Group ID: " + groupId);
        return groupId;
	}
	
    public static void main(String[] args) {
    	CreateSecurityGroup("AwsAutoScaling", "15619 Project 2.1");
    }
}
