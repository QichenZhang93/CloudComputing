package project1_1_15619_analyzer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;

public class Analyzer {
	
	public static String Filepath = "";
	
	protected static int ans2() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream("./pagecounts-20160501-000000"));
		int sum = 0;
		Scanner lineScanner = null;
		while (scanner.hasNextLine()) {
			/*
			 * 1: suppose dirty row's col num < 4
			 * 2: title is a number
			 */
			String thisLine = scanner.nextLine();
			//lineScanner = new Scanner(thisLine);
			String[] arr = thisLine.split("\\s+");
			if (arr.length < 4) {
				sum += Integer.valueOf(arr[1]);
			}
			else {
				sum += Integer.valueOf(arr[2]);
			}
			/*
			while (lineScanner.hasNext()) {
				if (lineScanner.hasNextInt()) {
					sum += lineScanner.nextInt();
					break;
				}
				else {
					lineScanner.next(); // skip
				}
			}
			*/
			/*
			int int_num = 0;
			int[] ints = new int[4];
			while (lineScanner.hasNext()) {
				if (lineScanner.hasNextInt()) {
					ints[int_num] = lineScanner.nextInt();
					int_num += 1;
				}
				else {
					int_num = 0;
					lineScanner.next();
				}
			}
			switch (int_num) {
			case 1: // total_count is missing
				sum += ints[int_num - 1];
				break;
			case 2: // standard
				sum += ints[0];
				break;
			case 3: // page_title is a number
				sum += ints[1];
			default:
				break;
			}*/
		}
		scanner.close();
		return sum;
	}
	
	// filtered output
	protected static String ans3() throws FileNotFoundException {
		// most popular title
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		String title = "";
		while (scanner.hasNextLine()) {
			String firstLine = scanner.nextLine();
			String[] str = firstLine.split("\\s+");
			if (str.length <= 0) continue;
			title = str[0];
			break;
		}
		scanner.close();
		return  title;
	}
	
	protected static int ans4() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		int cnt_views = 0;
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			if (row.length == 0) continue;
			String title = row[0];
			if (title.matches(".*film.*")) {
				cnt_views = Integer.valueOf(row[1]);
				break;
			}
		}
		scanner.close();
		return cnt_views;
	}
	
	protected static int ans5() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		int num_titles = 0;
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			String title = row[0];
			if (title.matches(".*([Cc][Ll][Oo][Uu][Dd])+.*") && title.matches(".*([Cc][Oo][Mm][Pp][Uu][Tt][Ii][Nn][Gg])+.*")) {
				num_titles += 1;
			}
		}
		scanner.close();
		return num_titles;
	}
	
	protected static int ans6() throws FileNotFoundException  {
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		int num = 0;
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			if (row.length == 0) continue;
			String title = row[0];
			if (title.length() < 2) continue;
			if (title.matches("^[0-9][A-Za-z].*")) {
				num += Integer.valueOf(row[1]);
			}
		}
		scanner.close();
		return num;
	}
	
	protected static int ans7() throws FileNotFoundException {
		HashSet<String> TVnames = new HashSet<>();
		int num = 0;
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			if (row[0].matches(".*_\\((\\d\\d\\d\\d_)?TV_series\\)$")) { // Casesensitive?
				String name = row[0];
				// Get the name of TV series
				name = name.substring(0, name.lastIndexOf("_("));
				TVnames.add(name);
			}
		}
		scanner.close();
		scanner = new Scanner(new FileInputStream(Filepath));
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			if (row[0].matches(".*_\\((\\d\\d\\d\\d_)?film\\)$")) {
				String name = row[0];
				// Get the name of TV series
				name = name.substring(0, name.lastIndexOf("_("));
				if (TVnames.contains(name)) {
					++num;
				}
			}
		}
		scanner.close();
		return num;
	}
	
	protected static int ans8() throws FileNotFoundException {
		Scanner scanner = new Scanner(new FileInputStream(Filepath));
		int num = 0;
		while (scanner.hasNextLine()) {
			String[] row = scanner.nextLine().split("\t+");
			if (row[0].matches("(^|.*[^a-zA-Z])[Cc][Ll][Oo][Uu][Dd]([^a-zA-Z].*|$)")) { // Casesensitive
				++num;
			}
		}
		scanner.close();
		return num;
	}
	
	
	public static void main(String args[]) throws FileNotFoundException {
		if (args.length <= 1) return;
		Filepath = args[1];
		int question_index = Integer.valueOf(args[0]);
		if (question_index < 2 || question_index > 8) return;
		switch (question_index) {
		case 2:
			System.out.print(ans2());
			break;
		case 3:
			System.out.print(ans3());
			break;
		case 4:
			System.out.print(ans4());
			break;
		case 5:
			System.out.print(ans5());
			break;
		case 6:
			System.out.print(ans6());
			break;
		case 7:
			System.out.print(ans7());
			break;
		case 8:
			System.out.print(ans8());
			break;
		default:
			break;
		}
		
	}
}
