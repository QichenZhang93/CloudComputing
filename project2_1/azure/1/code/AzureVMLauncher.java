package project2_1;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import com.microsoft.azure.Azure;
import com.microsoft.azure.CloudException;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.storage.StorageAccount;

public class AzureVMLauncher {
	
	public static int DataCenterNumber = 0; // The first one has already been built
	static final String DATA_CENTER = "qichenzdatacenter";
	static int LOAD_GENERATOR_N = 0; // Make sure the public address is unique
	static final String LOAD_GENERATOR = "qichenzloadgen";
	static int DATA_CENTER_N = 1; //// Make sure the public address is unique
	static final String RESOURCE_GROUP = "Rgroup";
	
	static public HashSet<String> DataCentersDns = new HashSet<String>();
	
	// return null if not successful. else return created vm.
	public static VirtualMachine LaunchNewLoadGenerator(Azure azure) {
		VirtualMachine virtualMachine = null;
		try {
			StorageAccount storageAccount = azure.storageAccounts().getByGroup("Rgroup", "qichenzcmu");
			while (true) {
				try {
					virtualMachine = azure.virtualMachines().define(LOAD_GENERATOR)
						.withRegion(Region.US_EAST)
						.withExistingResourceGroup("Rgroup")
						.withNewPrimaryNetwork("10.0.0.0/28")
						.withPrimaryPrivateIpAddressDynamic()
						.withNewPrimaryPublicIpAddress(LOAD_GENERATOR + LOAD_GENERATOR_N)
						.withStoredWindowsImage("https://qichenzcmu.blob.core.windows.net/system/Microsoft.Compute/Images/vhds/lgp21f16v3-osDisk.7f5686b8-d969-4bb7-ab57-2e079071e923.vhd")
						.withAdminUserName("qichenz")
						.withPassword("Zhang@UbuntuAzure")
						.withExistingStorageAccount(storageAccount)
						.withTag("Project", "2.1")
						.withSize(VirtualMachineSizeTypes.STANDARD_D1)
						.create();
					break;
				}
				catch (CloudException e) {
					e.printStackTrace();
					++LOAD_GENERATOR_N; // CloudException is caused by duplicate ip address, so far
					// Error code 409: Changing property 'osDisk.vhd.uri' is not allowed.
					// This is caused by creating the second load generator.
					continue;
				}
				catch (SocketTimeoutException e) {
					e.printStackTrace();
					continue;
				}
			}
			
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return virtualMachine;
	}
	
	final static String DC_IMAGE_LOCATION = "https://qichenzcmu.blob.core.windows.net/system/Microsoft.Compute/Images/vhds/cc15619p21dcv5-osDisk.e27faca3-f177-40ea-a740-9a1838326ae6.vhd";
	// return null if not successful. else return created vm.
	public static VirtualMachine LaunchNewDataCenter(Azure azure) {
		VirtualMachine virtualMachine = null;
		try {
			StorageAccount storageAccount = azure.storageAccounts().getByGroup("Rgroup", "qichenzcmu");
			while (true) {
				try {
					virtualMachine = azure.virtualMachines().define(DATA_CENTER + DATA_CENTER_N)
						.withRegion(Region.US_EAST)
						.withExistingResourceGroup("Rgroup")
						.withNewPrimaryNetwork("10.0.0.0/28")
						.withPrimaryPrivateIpAddressDynamic()
						.withNewPrimaryPublicIpAddress(DATA_CENTER + DATA_CENTER_N)
						.withStoredWindowsImage(DC_IMAGE_LOCATION)
						.withAdminUserName("qichenz")
						.withPassword("Zhang@UbuntuAzure")
						.withExistingStorageAccount(storageAccount)
						.withTag("Project", "2.1")
						.withSize(VirtualMachineSizeTypes.STANDARD_A1)
						.create();
					++DataCenterNumber;
					++DATA_CENTER_N;
					break;
				}
				catch (CloudException e) {
					e.printStackTrace();
					++DATA_CENTER_N; // CloudException is caused by duplicate ip address, so far
					continue;
				}
				catch (SocketTimeoutException e) {
					e.printStackTrace();
					continue;
				}
			}
			
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return virtualMachine;
	}
}
