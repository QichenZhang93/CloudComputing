import java.awt.geom.Ellipse2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Scanner;

public class q7 {

	public static class OSInfo {
		public String name;
		public int totalViews;
		public OSInfo() {
			name = "";
			totalViews = 0;
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		ArrayList<OSInfo> osInfos = new ArrayList<>();
		HashSet<String> osNames = new HashSet<>();
		String osNamePath = args[0];
		String inputpath = args[1];
		Scanner osNameScanner = new Scanner(new File(osNamePath), "UTF-8");
		Scanner scanner = new Scanner(new File(inputpath), "UTF-8");
		while (osNameScanner.hasNextLine()) {
			osNames.add(osNameScanner.nextLine());
		}
		osNameScanner.close();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			if (osNames.contains(items[1])) {
				OSInfo osInfo= new OSInfo();
				osInfo.name = items[1];
				osInfo.totalViews = Integer.parseInt(items[0]);
				osInfos.add(osInfo);
			}
		}
		scanner.close();
		osInfos.sort(new Comparator<OSInfo>() {
			@Override
			public int compare(OSInfo o1, OSInfo o2) {
				if (o1.totalViews !=  o2.totalViews) {
					return -(o1.totalViews - o2.totalViews);
				}
				else return o2.name.compareTo(o1.name);
			}
		});
		
		boolean notFirst = false;
		for (OSInfo movieInfo : osInfos) {
			if (notFirst) {
				System.out.print(',');
			}
			notFirst = true;
			System.out.print(movieInfo.name);
		}
	}

}
