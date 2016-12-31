package project2_1.aws;

import java.util.ArrayList;
import java.util.List;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.InstanceStatus;

public class AwsVMLauncher {
	static private final String SECURITY_GROUP = "AwsAutoScaling";
	
	private ArrayList<Tag> tags = new ArrayList<Tag>();
	
	public AwsVMLauncher() {
		tags.add(new Tag("Project", "2.1"));
	}
	
	private Instance LaunchInstance(AmazonEC2Client ec2Client, String imageId) {
		
		RunInstancesRequest request = new RunInstancesRequest();
		request.withImageId(imageId)
			.withInstanceType(InstanceType.M3Medium) // TODO: change this to m3Medium
			.withMaxCount(1)
			.withMinCount(1)
			.withSecurityGroups(SECURITY_GROUP)
			.withKeyName("15619_CC");
		RunInstancesResult result = ec2Client.runInstances(request);
		List<Instance> instances = result.getReservation().getInstances();
		
		// Set tags
		Instance newInstance = instances.get(0);
		String instanceId = newInstance.getInstanceId();
		ArrayList<String> instanceIdList = new ArrayList<String>();
		instanceIdList.add(instanceId);
		ec2Client.createTags(new CreateTagsRequest(instanceIdList, tags));
		
		// makesure that this instance is running
		while (true) {
			DescribeInstanceStatusRequest describeInstanceStatusRequest = new DescribeInstanceStatusRequest();
			describeInstanceStatusRequest.setInstanceIds(instanceIdList);
			DescribeInstanceStatusResult describeInstanceStatusResult = ec2Client.describeInstanceStatus(describeInstanceStatusRequest);
			List<InstanceStatus> statusList = describeInstanceStatusResult.getInstanceStatuses(); // The size is 1
			if (statusList.size() == 0) continue;
			InstanceStatus status = statusList.get(0);
			if (status.getInstanceState().getCode() == 16) break;
			try {
				System.out.println("New instence is not ready");
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
		describeInstancesRequest.setInstanceIds(instanceIdList);
		DescribeInstancesResult describeInstancesResult = ec2Client.describeInstances(describeInstancesRequest);
		newInstance = describeInstancesResult.getReservations().get(0).getInstances().get(0);
		return newInstance;
	}
	
	public Instance LaunchNewLoadGenerator(AmazonEC2Client ec2Client) {
		System.out.println("Creating LG!");
		Instance instance = LaunchInstance(ec2Client, "ami-08710c1f");
		System.out.println("Load generator ready!");
		return instance;
	}
	
	public Instance LaunchNewDataCenter(AmazonEC2Client ec2Client) {
		System.out.println("Creating DC!");
		Instance instance = LaunchInstance(ec2Client, "ami-ebd5a9fc");
        System.out.println("DC ready!");
		return instance;
	}
}
