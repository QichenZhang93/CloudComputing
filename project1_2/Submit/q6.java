import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Scanner;

public class q6 {

	public static class MovieInfo {
		public String name;
		public int maxViewsSingleDay;
		public MovieInfo() {
			name = "";
			maxViewsSingleDay = 0;
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		ArrayList<MovieInfo> movies = new ArrayList<>();
		HashSet<String> movieNames = new HashSet<>();
		String movieNamePath = args[0];
		String inputName = args[1];
		Scanner movieNameScanner = new Scanner(new File(movieNamePath), "UTF-8");
		while (movieNameScanner.hasNextLine()) {
			movieNames.add(movieNameScanner.nextLine());
		}
		movieNameScanner.close();
		Scanner scanner = new Scanner(new File(inputName), "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] items = line.split("\\s+");
			if (movieNames.contains(items[1])) {
				MovieInfo movieInfo= new MovieInfo();
				movieInfo.name = items[1];
				for (int i = 2; i < items.length; ++i) {
					int viewsCount = Integer.parseInt(items[i].split(":")[1]);
					if (viewsCount > movieInfo.maxViewsSingleDay) {
						movieInfo.maxViewsSingleDay = viewsCount;
					}
				}
				movies.add(movieInfo);
			}
		}
		scanner.close();
		movies.sort(new Comparator<MovieInfo>() {
			@Override
			public int compare(MovieInfo o1, MovieInfo o2) {
				return -(o1.maxViewsSingleDay - o2.maxViewsSingleDay);
			}
		});
		
		boolean notFirst = false;
		for (MovieInfo movieInfo : movies) {
			if (notFirst) {
				System.out.print(',');
			}
			notFirst = true;
			System.out.print(movieInfo.name);
		}
	}

}
