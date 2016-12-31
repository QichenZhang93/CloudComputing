package project2_1.aws;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ini4j.Wini;
import org.ini4j.Profile.Section;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;

public class AwsHorizontalScaling {
	
	static String AndrewId = "";
	static String SubmissionPassword = "";
	static String LoadGeneratorDns = "ec2-52-201-213-150.compute-1.amazonaws.com";
	static String FirstDataCenterDNS = "ec2-54-209-32-96.compute-1.amazonaws.com";
	static String TestIdPartOfLogUrl = "";
	static final double ObjectRPS = 4000.000;
	static int DataCenterNumber = 0;
	
	/*
	 * Submit password and andrew id to let load generator run
	 */
	static boolean StartLoadGenerator(String loadGeneratorDns, String submissionPassword, String andrewId) {
		System.out.println("Start Load generator: " + loadGeneratorDns);
		while (true) {
			String url = "http://" + loadGeneratorDns + "/password?passwd=" + submissionPassword + "&andrewid=" + andrewId;
			try
			{
				URLConnection connection = new URL(url).openConnection();
				connection.setRequestProperty("Accept-Charset", "UTF-8");
				InputStream response = connection.getInputStream();
				/////
				Scanner scanner = new Scanner(response);
				scanner.useDelimiter("\\A");
				while (scanner.hasNextLine()) {
					System.out.println(scanner.nextLine());
				}
				scanner.close();
				return true;
			}
			catch (IOException e) {
				e.printStackTrace();
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				continue;
			}
		}
		
	}
	
	/*
	 * Start the test by submitting the 1st data center vm
	 */
	static boolean SubmitFirstDataCenter(String dataCenterDns, String loadGeneratorDns) {
		System.out.println("Submit first datacenter: " + dataCenterDns);
		// Proceed after the Data center is ready. Need to wait for a few minutes
		while (true) {
		try {
				String url = "http://" + loadGeneratorDns + "/test/horizontal?dns=" + dataCenterDns;
				URLConnection connection = new URL(url).openConnection();
				InputStream response = connection.getInputStream(); // Will return a HTML page with test id embedded in it.
				Scanner scanner = new Scanner(response);
				scanner.useDelimiter("\\A");
				while (scanner.hasNextLine()) {
					String htmlLine = scanner.nextLine();
					System.out.println(htmlLine);
					// <!DOCTYPE html><html><head><title>MSB Load Generator</title></head><body><a href='/log?name=test.1474482692026.log'>Test</a> launched.</body></html>
					int index1 = htmlLine.indexOf("<a href='");
					int index2 = htmlLine.indexOf("'>Test</a>");
					TestIdPartOfLogUrl = htmlLine.substring(index1 + 9, index2); // '/log?name=test.xxxxx.log'
				}
				scanner.close();
				DataCenterNumber += 1;
				return true;
			}
			catch (IOException e) {
				e.printStackTrace(); // Get 400 because DC is not ready in that short time. Need to deal with this.
				// Wait for a few minutes before proceeding
				try {
					System.out.println("DataCenter not ready.");
					Thread.sleep(10 * 1000); // wait for 10 seconds
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				continue;
			}
		}
	}
	
	static class DataCenterWorker implements Runnable {
		String dataCenterDns;
		Instance newDataCenter;
		AmazonEC2Client ec2Client;
		public DataCenterWorker(AmazonEC2Client ec2Client) {
			this.ec2Client = ec2Client;
		}
		public void run() {
			newDataCenter = new AwsVMLauncher().LaunchNewDataCenter(ec2Client);
			dataCenterDns = newDataCenter.getPublicDnsName();
			AddDataCenter(dataCenterDns, LoadGeneratorDns);
		}
	}
	
	static void AddMultipleDataCenters(AmazonEC2Client ec2Client, int number) {
		System.out.println("AddMultipleDataCenters: " + number);
		ExecutorService executorService = Executors.newFixedThreadPool(number);
		boolean isFirst = true;
		System.out.println("StartAutoScalling-launch & add new datacenters");
		for (int i = 0; i < number; ++i) {
			if (!isFirst) {
				try {
					Thread.sleep(100000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			isFirst = false;
			executorService.execute(new DataCenterWorker(ec2Client));
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} }
		System.out.println("AddMultipleDataCenters ends.");
	}
	
	static void StartAutoScaling(AmazonEC2Client ec2Client) throws InterruptedException {
		System.out.println("StartAutoScalling");
		double rps = -1;
		Thread.sleep(60000);
		while (true) {
			do {
				rps = ReadCurrentRpsFromLog(LoadGeneratorDns, TestIdPartOfLogUrl);
				if (rps < 0) Thread.sleep(10000);
			} while (rps < 0);
			
			if (rps > ObjectRPS) break;
			
			// Estimate how many more datacenters are needed 			
			double averageRps = (rps / DataCenterNumber);
			double remainingRps = ObjectRPS - rps;
			int estimatedNumber = (int)(remainingRps / averageRps);
			if (estimatedNumber <= 0) estimatedNumber = 1;
			
			AddMultipleDataCenters(ec2Client, estimatedNumber);
		}
	}
	
	/* 
	 * Monitor log from http://[your-load-generator-instance-dns-name]/log?name=test.[test-number].log
	 * and add data center vm accordingly.
	 */
	static double ReadCurrentRpsFromLog(String loadGeneratorDns, String subDirOfTest) {
		//System.out.println("ReadLog");
		try {
			String url = "http://" + loadGeneratorDns + subDirOfTest;
			URLConnection connection = new URL(url).openConnection();
			InputStream response = connection.getInputStream();
			
			double rps = 0;
			
			// Get rps
			Wini iniFile = new Wini(response);
			int iniFileSize = iniFile.size();
			if (iniFileSize < 3) return -1;
			System.out.println("ReadLog");
			String secName = "Minute " + (iniFileSize / 2);
			Section section = iniFile.get(secName);
			for (Entry<String, String> entry : section.entrySet()) {
				rps += (Double.valueOf(entry.getValue()));
			}
			System.out.println("rps: " + rps);
			return rps;
		} catch (IOException e) {
			e.printStackTrace(); // the log is not ready
			return -1;
		}
	}
	
	static void AddDataCenter(String dataCenterDns, String loadGeneratorDns) {
		System.out.println("AddDataCenter: " + dataCenterDns);
		// http://[your-load-generator-instance-dns-name]/test/horizontal/add?dns=[your-instance-dns-name]
		// Proceed after the Data center is ready. Need to wait for a few minutes
		while (true) {
		try {
				String url = "http://" + loadGeneratorDns + "/test/horizontal/add?dns=" + dataCenterDns;
				URLConnection connection = new URL(url).openConnection();
				InputStream response = connection.getInputStream();
				Scanner scanner = new Scanner(response);
				scanner.useDelimiter("\\A");
				while (scanner.hasNextLine()) {
					System.out.println(scanner.nextLine());
				}
				scanner.close();
				DataCenterNumber += 1;
				break;
			}
			catch (IOException e) {
				e.printStackTrace(); // Get 400 because DC is not ready in that short time. Need to deal with this.
				// Wait for a few minutes before proceeding
				try {
					System.out.println("DataCenter not ready.");
					Thread.sleep(30 * 1000); // wait for 30 seconds
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				continue;
			}
		}
	}
	
	// Get andrew Id and submission password from file
	public static void GetPersonalInfo() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("/home/qichen/Documents/15619/Azure/andrewid"));
		while (scanner.hasNextLine()) {
			AndrewId = scanner.nextLine();
		}
		scanner.close();
		scanner = new Scanner(new File("/home/qichen/Documents/15619/Azure/submissionpw2_1"));
		while (scanner.hasNextLine()) {
			SubmissionPassword = scanner.nextLine();
		}
		scanner.close();
		System.out.println("Get Personal Info! ID: " + AndrewId + " SUB_PW: " + SubmissionPassword);
	}
	
	// Set up credentials	
	public static AmazonEC2Client SetUpClient() {
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
		AmazonEC2Client ec2 = new AmazonEC2Client(credentials);
		Region region = Region.getRegion(Regions.US_EAST_1);
		ec2.setRegion(region);
		return ec2;
	}
	
	public static void main(String[] args) throws Exception {
		AmazonEC2Client ec2Client = SetUpClient();
		AwsVMLauncher launcher = new AwsVMLauncher();
		GetPersonalInfo();
        StartLoadGenerator(LoadGeneratorDns, SubmissionPassword, AndrewId); // Hard coded lG DNS
        SubmitFirstDataCenter(FirstDataCenterDNS, LoadGeneratorDns); // Hard coded 1st DC DNS
        StartAutoScaling(ec2Client);
		//System.out.println(launcher.LaunchNewDataCenter(ec2Client).getPublicDnsName());
	}
}


////////////Test id: 1474593347877////////////////
