import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class q4 {

	public static void main(String[] args) throws FileNotFoundException {
		String filename = args[0];
		Scanner scanner = new Scanner(new File(filename), "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			String viewsOnMay1 = items[2];
			viewsOnMay1 = viewsOnMay1.split(":")[1];
			int viewsCount = Integer.parseInt(viewsOnMay1);
			if (viewsCount == 0) {
				System.out.print(items[1]);
				break;
			}
		}
		scanner.close();
	}

}
