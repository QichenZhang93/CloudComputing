import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class q8 {
	
	/*
	# When did the article "NBA_Finals" have the most number of page views?
	# Input the answer in yyyymmdd format
	# Run your commands/code to process the output and echo the answer
	# in the above format to standard output
	 */

	public static void main(String[] args) throws FileNotFoundException {
		String filename = args[0];
		String articleName = "NBA_Finals";
		Scanner scanner = new Scanner(new File(filename), "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			String aName = items[1];
			if (aName.equals(articleName)) {
				int maxViews = -1;
				String date = "";
				for (int i = 2; i < items.length; ++i) {
					String[] date_views = items[i].split(":");
					int tempViews = Integer.parseInt(date_views[1]);
					if (tempViews > maxViews) {
						maxViews = tempViews;
						date = date_views[0];
					}
				}
				System.out.print(date);
				break;
			}
		}
		scanner.close();
 	}
}