package project2_1.azure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.security.sasl.SaslClient;

import org.bouncycastle.pqc.crypto.gmss.util.WinternitzOTSignature;
import org.ini4j.Ini;
import org.ini4j.Wini;
import org.ini4j.Profile.Section;
import org.omg.CORBA.Current;

import com.microsoft.azure.Azure;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.rest.credentials.ServiceClientCredentials;

public class AzureAutoScalingWithCode {
	
	static  String ANDREW_ID = "";
	static  String SUBMISSION_PASSWORD = "";
	static String LoadGeneratorDNS = "qichenzloadgen0.eastus.cloudapp.azure.com";
	static String FirstDataCenterDNS = "qichenzdatacenter0.eastus.cloudapp.azure.com";
	static String TestIdPartOfLogUrl = "";
	static final double ObjectRPS = 3000.000;
	
	public static ServiceClientCredentials GetAwsCredentials() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(new File("/home/qichen/Documents/15619/Azure/hello")), "UTF-8");
		String appId = "";
		String tenId = "";
		String key = "";
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			if (items[0].equals("ApplicationID")) {
				appId = items[1];
			}
			else if (items[0].equals("ApplicationKey")) {
				key = items[1];
			}
			else if (items[0].equals("TenantID")) {
				tenId = items[1];
			}
		}
		scanner.close();
		return new ApplicationTokenCredentials(appId, tenId, key, AzureEnvironment.AZURE);
	}
	public static String GetSubscription() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(new File("/home/qichen/Documents/15619/Azure/subscription")), "UTF-8");
		String subsId = "";
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			if (items[0].equals("Subscription")) {
				subsId = items[1];
			}
		}
		scanner.close();
		return subsId;
	}
	
	static boolean SetUpLoadGenerator(Azure azure) {
		VirtualMachine virtualMachine = null;
		while (true) {
			virtualMachine = AzureVMLauncher.LaunchNewLoadGenerator(azure);
			if (virtualMachine == null) continue;
			while (true) {
				try {
					System.out.println(String.format("LoadGenerator created! ID: %s, IP: %s, fqdn: %s, leafDomainLabel: %s\n",
							virtualMachine.id(),
							virtualMachine.primaryPublicIpAddress().ipAddress(),
							virtualMachine.primaryPublicIpAddress().fqdn(),
							virtualMachine.primaryPublicIpAddress().leafDomainLabel()));
					LoadGeneratorDNS = virtualMachine.primaryPublicIpAddress().fqdn();
					break;
				}
				catch (CloudException e) { // CloudException is caused by not carefully configuring the cloud
					e.printStackTrace();
					return false;
				}
				catch (IOException e) { // Connection problem is usually solved by making request later
					e.printStackTrace();
					continue;
				}
			}
			break;
		}
		return true;
	}
	
	static boolean SetUpFirstDataCenter(Azure azure) {
		VirtualMachine virtualMachine = null;
		
		while(true) {
			virtualMachine = AzureVMLauncher.LaunchNewDataCenter(azure);
			if (virtualMachine == null) continue;
			while (true) {
				try {
					//dataCenters.add(virtualMachine);
					String dns = virtualMachine.primaryPublicIpAddress().fqdn();
					System.out.println(String.format("Datacenter created! ID: %s, IP: %s, fqdn: %s, leafDomainLabel: %s\n",
							virtualMachine.id(),
							virtualMachine.primaryPublicIpAddress().ipAddress(),
							dns,
							virtualMachine.primaryPublicIpAddress().leafDomainLabel()));
					break;
				}
				catch (CloudException e) { // CloudException is caused by not carefully configuring the cloud
					e.printStackTrace();
					return false;
				}
				catch (IOException e) { // Connection problem is usually solved by making request later
					e.printStackTrace();
					continue;
				}
			}
			break;
		}

		return true;
	}
	
	/*
	 * Start the load generator VM and the 1st data center VM
	 */
	static boolean SetUpVMs(Azure azure) {
		boolean isNextStepSuccessful = false;
		do {
			isNextStepSuccessful = SetUpLoadGenerator(azure);
		} while (!isNextStepSuccessful);
		
		do {
			isNextStepSuccessful = SetUpFirstDataCenter(azure);
		} while (!isNextStepSuccessful);
		return true;
	}
	////******************** Done before testing ********************////
	
	
	/*
	 * Submit password and andrew id to let load generator run
	 */
	static boolean StartLoadGenerator(String loadGeneratorDns) {
		System.out.println("Start Load generator: " + loadGeneratorDns);
		String url = "http://" + loadGeneratorDns + "/password?passwd=" + SUBMISSION_PASSWORD + "&andrewid=" + ANDREW_ID;
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
			/////
			return true;
		}
		catch (IOException e) {
			e.printStackTrace();
			return false;
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
				AzureVMLauncher.DataCenterNumber = 1;
				AzureVMLauncher.DataCentersDns.add(dataCenterDns);
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
		VirtualMachine newDataCenter;
		Azure azure;
		public DataCenterWorker(Azure azure) {
			this.azure = azure;
		}
		public void run() {
			do {
				newDataCenter = AzureVMLauncher.LaunchNewDataCenter(azure);
			} while (newDataCenter == null);
			
			while (true) {
				try {
					dataCenterDns = newDataCenter.primaryPublicIpAddress().fqdn();
					break;
				}
				catch (Exception e) {
					continue;
				}
			}
			
			boolean isNextStepSuccessful = false;
			do {
				isNextStepSuccessful = AddDataCenter(dataCenterDns, LoadGeneratorDNS);
			} while (!isNextStepSuccessful);
		}
	}
	
	static void AddMultipleDataCenters(Azure azure, int number) {
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
			executorService.execute(new DataCenterWorker(azure));
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) { }
		System.out.println("AddMultipleDataCenters ends.");
	}
	
	static void StartAutoScaling(Azure azure) throws InterruptedException {
		System.out.println("StartAutoScalling");
		double rps = -1;
		while (true) {
			do {
				rps = ReadCurrentRpsFromLog(LoadGeneratorDNS, TestIdPartOfLogUrl);
				if (rps < 0) Thread.sleep(10000);
			} while (rps < 0);
			
			if (rps > ObjectRPS) break;
			
			// Estimate how many more datacenters are needed 			
			double averageRps = (rps / AzureVMLauncher.DataCentersDns.size());
			double remainingRps = ObjectRPS - rps;
			int estimatedNumber = (int)(remainingRps / averageRps);
			if (estimatedNumber <= 0) estimatedNumber = 1;
			
			AddMultipleDataCenters(azure, estimatedNumber);
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
			if (iniFile.size() <= 1) return -1; System.out.println("ReadLog");
			String secName = "Minute " + (iniFile.size() - 1);
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
	
	static boolean AddDataCenter(String dataCenterDns, String loadGeneratorDns) {
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
				AzureVMLauncher.DataCentersDns.add(dataCenterDns);
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
		return true;
	}
	
	public static void RunAutoScalingTest() throws FileNotFoundException, InterruptedException {
		ServiceClientCredentials credentials = GetAwsCredentials();
		Azure azure = Azure.authenticate(credentials).withSubscription(GetSubscription());
		
		boolean isNextStepSuccessful = false;
		
		do {
			isNextStepSuccessful = StartLoadGenerator(LoadGeneratorDNS); // PASS!
		}
		while (!isNextStepSuccessful);
		
		// Submit the firat datacenter to start the test
		SubmitFirstDataCenter(FirstDataCenterDNS, LoadGeneratorDNS);
		
		StartAutoScaling(azure);
		
		System.out.println("Complete!");
	}
	
	// Get andrew Id and submission password from file
	public static void GetPersonalInfo() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("/home/qichen/Documents/15619/Azure/andrewid"));
		while (scanner.hasNextLine()) {
			ANDREW_ID = scanner.nextLine();
		}
		scanner.close();
		scanner = new Scanner(new File("/home/qichen/Documents/15619/Azure/submissionpw2_1"));
		while (scanner.hasNextLine()) {
			SUBMISSION_PASSWORD = scanner.nextLine();
		}
		scanner.close();
		System.out.println("Get Personal Info! ID: " + ANDREW_ID + " SUB_PW: " + SUBMISSION_PASSWORD);
	}
	
	public static void main(String[] args) throws Exception {
		GetPersonalInfo();
		RunAutoScalingTest();
	}
}
