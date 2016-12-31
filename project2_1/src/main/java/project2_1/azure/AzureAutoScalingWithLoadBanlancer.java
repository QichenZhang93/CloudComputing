package project2_1.azure;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

public class AzureAutoScalingWithLoadBanlancer {

	public static void main(String[] args) throws MalformedURLException, InterruptedException {
		URL url = new URL("http://qichenz1azure1project21.eastus.cloudapp.azure.com/lookup/random");
		while (true) {
			try {
				URLConnection connection = url.openConnection();
				InputStream response = connection.getInputStream();
				Scanner scanner = new Scanner(response);
				while (scanner.hasNext()) {
					System.out.println(scanner.nextLine());
				}
				scanner.close();
				break;
			}
			catch (IOException e) {
				e.printStackTrace();
				Thread.sleep(20000);
				continue;
			}
		}
		url = new URL("http://qichenzloadgen0.eastus.cloudapp.azure.com/junior?dns=qichenz1azure1project21.eastus.cloudapp.azure.com");
		while (true) {
			try {
				URLConnection connection = url.openConnection();
				InputStream response = connection.getInputStream();
				Scanner scanner = new Scanner(response);
				while (scanner.hasNext()) {
					System.out.println(scanner.nextLine());
				}
				scanner.close();
				break;
			}
			catch (IOException e) {
				e.printStackTrace();
				Thread.sleep(20000);
				continue;
			}
		}
		
	}

}
