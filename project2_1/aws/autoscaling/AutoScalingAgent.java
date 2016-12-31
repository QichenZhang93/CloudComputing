package project2_1.autoscaling;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.applicationautoscaling.model.StepAdjustment;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import project2_1.aws.AwsSecurityGroup;

public class AutoScalingAgent {
	
	private static final String DC_IMAGE_ID = "ami-ebd5a9fc";
	private static final String DC_INSTANCE_TYPE = "m3.medium";
	private static final String LAUNCH_CONFIG_NAME = "15619project21launchconfig";
	
	private static void SetUpLaunchConfiguration(AmazonAutoScalingClient client) {
		System.out.println("Set up Launch Configuration");
		CreateLaunchConfigurationRequest request = new CreateLaunchConfigurationRequest();
		request.withImageId(DC_IMAGE_ID).withInstanceType(DC_INSTANCE_TYPE)
			.withSecurityGroups(AwsSecurityGroup.SECURITY_GROUP_NAME1)
			.withLaunchConfigurationName(LAUNCH_CONFIG_NAME);
		client.createLaunchConfiguration(request);
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Done setting up Launch Configuration");
	}
	
	private static final String AUTOSCALING_GROUP_NAME = "15619project21autoscaling";
	private static final String SCALING_POLICY_NAME_INCREASE = "scalingpolicynameincrease";
	private static final String SCALING_POLICY_NAME_DECREASE = "scalingpolicynamedecrease";
	
	private static String[] SetUpAutoScalingGroup(AmazonAutoScalingClient client) {
		System.out.println("Set up Auto Scaling group");
		
		CreateAutoScalingGroupRequest request = new CreateAutoScalingGroupRequest();
		Tag tag = new Tag();
		tag.setKey("Project");
		tag.setValue("2.1");
		request.withAutoScalingGroupName(AUTOSCALING_GROUP_NAME)
			.withAvailabilityZones(AwsAutoScaling.AVALIBILITY_ZONE)
			.withDefaultCooldown(300)
			.withDesiredCapacity(1)
			.withHealthCheckType("EC2")
			.withHealthCheckGracePeriod(120)
			.withTerminationPolicies("Default")
			.withLaunchConfigurationName(LAUNCH_CONFIG_NAME)
			.withLoadBalancerNames(ElbAgent.LOAD_BANLANCER_NAME)
			.withMaxSize(20)
			.withMinSize(1)
			.withTags(tag);
		client.createAutoScalingGroup(request);
		
		// Polices TODO: Problem here --> StepScaling vs SimpleScaling
		PutScalingPolicyRequest scalingPolicyRequest = new PutScalingPolicyRequest();
		scalingPolicyRequest
			.withAdjustmentType("ChangeInCapacity")
			.withAutoScalingGroupName(AUTOSCALING_GROUP_NAME)
			.withCooldown(60)
			.withEstimatedInstanceWarmup(100)
			.withPolicyName(SCALING_POLICY_NAME_INCREASE)
			.withPolicyType("StepScaling")
			.withScalingAdjustment(1);
		
		String upArn = client.putScalingPolicy(scalingPolicyRequest).getPolicyARN();
		
		scalingPolicyRequest = new PutScalingPolicyRequest();
		scalingPolicyRequest
			.withAdjustmentType("ChangeInCapacity")
			.withAutoScalingGroupName(AUTOSCALING_GROUP_NAME)
			.withCooldown(60)
			.withEstimatedInstanceWarmup(100)
			.withPolicyName(SCALING_POLICY_NAME_DECREASE)
			.withPolicyType("StepScaling")
			.withScalingAdjustment(-1);
		
		String downArn = client.putScalingPolicy(scalingPolicyRequest).getPolicyARN();
		// TODO: Problem above
		
		
		String[] arns = {upArn, downArn};
		
		System.out.println("Done setting up Auto Scaling group");
		return arns;
	}
	
	private static void SetUpAlarm(AmazonCloudWatchClient client, String upArn, String downArn) {
		System.out.println("Set up alarm with arn: " + upArn + " " + downArn);
		PutMetricAlarmRequest upAlarm = new PutMetricAlarmRequest();
		Dimension dim = new Dimension();
		dim.withName("AutoScalingGroupName").withValue(AUTOSCALING_GROUP_NAME);
		upAlarm.withMetricName("CPUUtilization").withNamespace("AWS/EC2").withAlarmName("alarm-cpu-high-15619project21")
			.withDimensions(dim).withThreshold(90.0).withStatistic(Statistic.Average)
			.withPeriod(60).withComparisonOperator(ComparisonOperator.GreaterThanThreshold).withAlarmActions(upArn);
		client.putMetricAlarm(upAlarm);
		
		PutMetricAlarmRequest downAlarm = new PutMetricAlarmRequest();
		dim = new Dimension();
		dim.withName("AutoScalingGroupName").withValue(AUTOSCALING_GROUP_NAME);
		downAlarm.withMetricName("CPUUtilization").withNamespace("AWS/EC2").withAlarmName("alarm-cpu-low-15619project21")
			.withDimensions(dim).withThreshold(50.0).withStatistic(Statistic.Average)
			.withPeriod(60).withComparisonOperator(ComparisonOperator.LessThanOrEqualToThreshold).withAlarmActions(downArn);
		client.putMetricAlarm(downAlarm);
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Done setting up alarm with arn: " + upArn + " " + downArn);
		return;
	}
	
	public static void SetUpAutoScaling(AWSCredentials credentials) {
		System.out.println("Set up Autoscaling");
		AmazonAutoScalingClient scalingClient = new AmazonAutoScalingClient(credentials);
		SetUpLaunchConfiguration(scalingClient);
		String[] arns = SetUpAutoScalingGroup(scalingClient);
		//AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient(credentials);
		//SetUpAlarm(cloudWatchClient, arns[0], arns[1]);
		System.out.println("Done setting up Autoscaling");
	}
}
