import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class q5 {

	public static void main(String[] args) throws FileNotFoundException {
		String filename = args[0];
		String firstTitle = args[1];
		String[] firstViewsOverMonth = null;
		String secondTitle = args[2];
		String[] secondViewsOverMonth = null;
		//System.out.println(firstTitle + " " + secondTitle);
		int ans = 0;
		Scanner scanner = new Scanner(new File(filename), "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			if (firstTitle.equals(items[1])) {
				firstViewsOverMonth = items;
			}
			else if (secondTitle.equals(items[1])) {
				secondViewsOverMonth = items;
			}
			if ((firstViewsOverMonth != null) && (secondViewsOverMonth != null)) {
				break;
			}
		}
		if (firstViewsOverMonth == null) System.out.print(0);
		else if (secondViewsOverMonth == null) System.out.print(31);
		else {
			int i = 2;
			while (i <= 32) {
				if (Integer.parseInt(firstViewsOverMonth[i].split(":")[1]) > Integer.parseInt(secondViewsOverMonth[i].split(":")[1])) {
					++ans;
				}
				++i;
			}
			System.out.print(ans);
		}
		scanner.close();
	}

}
