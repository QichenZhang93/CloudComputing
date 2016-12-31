import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Reducer {

	class ArticleInfo {
		public int Date;
		public int ViewsCount;
		public ArticleInfo(int date, int viewsCount) {
			Date = date;
			ViewsCount = viewsCount;
		}
	}
	
	class SortArticle {
		public Integer ViewsCount;
		public String Date;
	}
	
	// TODO: Change to 100000 before running on server
	static final int THRESHOLD = 100000;
	
	public void PrintArticleInfo(long totalCount, String articleName, HashMap<String, Integer> articleViewsCount) throws IOException {
		if (totalCount <= THRESHOLD) return;
		ArrayList<String> keySetInSortedArray = new ArrayList<>(articleViewsCount.keySet());
		keySetInSortedArray.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				int int1 = Integer.parseInt(o1);
				int int2 = Integer.parseInt(o2);
				return int1 - int2;
			}
		});
		writer.write(totalCount + "\t" + articleName); // Print [total count][\t][article name]
		writer.flush();
		int currentDate = 0;
		int lastDate = 0;
		int dateInKey;
		for (String key : keySetInSortedArray) {
			dateInKey = Integer.parseInt(key); // Exception here. Input may be "datasets"
			if (currentDate == 0) { // Need to be initialized to the first day of the month
				currentDate = currentDate + (dateInKey / 100 * 100 + 1); // Set day to YYYYMM01.
			}
			if (lastDate == 0) {
				lastDate = currentDate + 30; // Assume all data comes from one month
			}
			
			while (currentDate < dateInKey) { // currentDate's views count is not in hashmap, meaning it's 0.
				writer.write("\t" + currentDate + ":" + 0); // Print [\t][date]:[views count(0)]
				++currentDate;
			}
				
			if (currentDate == dateInKey){
				writer.write("\t" + currentDate + ":" + articleViewsCount.get(key)); // Print [\t][date]:[views count]
				++currentDate;
			}
		}
		while (currentDate <= lastDate) {
			writer.write("\t" + currentDate + ":" + 0); // Print [\t][date]:[views count(0)]
			++currentDate;
		}
		writer.write('\n');
		writer.flush();
	}
	
	public void StoreViewsCount(String date, int viewsCount, HashMap<String, Integer> records) {
		if (records.containsKey(date)) {
			int previousCount = records.get(date);
			records.put(date, previousCount + viewsCount);
		}
		else {
			records.put(date, viewsCount);
		}
	}

	public BufferedReader reader = null;
	public BufferedWriter writer = null;
	
	public Reducer(InputStream in, OutputStream out) throws UnsupportedEncodingException {
		reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
	}
	
	public int Reduce() throws IOException {
		try {
			String input = null;
			HashMap<String, Integer> viewsCountInMonth = new HashMap<>(); // Key: date(yyyymmdd); Value: views count
			String currentArticleName = null; // Current article name
			//String articleNameWithDateFromStream = null; // the article name read from input stream
			String articleNameFromStream = null;
			String dateFromStream = null;
			String viewsCount_string = null;
			int viewsCount_int = 0;
			long totalCount = 0; // total views of an article within a month
			//System.out.println("Start");
			while ((input = reader.readLine()) != null) {
				//System.out.println("Stream: " + input);
				try {
					// Each line has format: articlename_date(yyyymmdd)[\t]views_count
					String[] items = input.split("\t+"); // item[0]:articlename  item[1]:date item[2]:views_count
					articleNameFromStream = items[0];
					dateFromStream = items[1];
					viewsCount_string = items[2]; //views_count (hourly)
					viewsCount_int = Integer.parseInt(viewsCount_string);
					// Aggregate hourly into daily
					if (currentArticleName == null) { // the first line
						currentArticleName = articleNameFromStream;
						StoreViewsCount(dateFromStream, viewsCount_int, viewsCountInMonth);
						totalCount = viewsCount_int;
					}
					else if (currentArticleName.equals(articleNameFromStream)) {
						StoreViewsCount(dateFromStream, viewsCount_int, viewsCountInMonth);
						totalCount += viewsCount_int;
						currentArticleName = articleNameFromStream;
					}
					else {
						PrintArticleInfo(totalCount, currentArticleName, viewsCountInMonth);
							// Move to the next article
							viewsCountInMonth.clear();
							currentArticleName = articleNameFromStream;
							//System.out.println(currentArticleNameWithDate + " " + viewsCount_int);
							StoreViewsCount(dateFromStream, viewsCount_int, viewsCountInMonth);
							totalCount = viewsCount_int;
					}
				}
				catch (NumberFormatException e) { // deal with bad entry
					// Actually the bad entry has been discarded by Mapper
					continue;
				}
			}
			// Deal with the last one
			// Print the last one!!
			PrintArticleInfo(totalCount, currentArticleName, viewsCountInMonth);
		}
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		writer.close();
		return 0;
	}
	
	/*
	 * The Reducer function
	 */
	public static void main(String[] args) throws IOException {
		Reducer reducer = new Reducer(System.in, System.out); // TODO: Uncomment this line before running on server
		//Reducer reducer = new Reducer(new FileInputStream("/home/qichen/workspace/project1_2_15619/src/testFile"), System.out);
		System.exit(reducer.Reduce());
	}

}
