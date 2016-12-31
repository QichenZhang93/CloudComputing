import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class q3 {

	public static void main(String[] args) throws FileNotFoundException {
		String filename = args[0];
		String article_name = "";
		int daily_views = 0;
		Scanner scanner = new Scanner(new File(filename), "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			String info = items[19];
			info = info.split(":")[1];
			int viewsCount = Integer.parseInt(info);
			if (article_name.equals("")) {
				daily_views = viewsCount;
				article_name = items[1];
			}
			else {
				if (viewsCount > daily_views) {
					daily_views = viewsCount;
					article_name = items[2];
				}
			}
		}
		scanner.close();
		System.out.print(article_name + "\t" + daily_views);
	}

}
