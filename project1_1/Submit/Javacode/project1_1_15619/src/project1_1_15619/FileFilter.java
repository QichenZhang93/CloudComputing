package project1_1_15619;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Scanner;

import javax.swing.tree.DefaultTreeCellEditor.EditorContainer;

public class FileFilter {
	
	class FilteredItem {
		public String PageTitle = "";
		public int CountViews = 0;
		public FilteredItem(String pageTitle, int countViews) {
			this.PageTitle = pageTitle;
			this.CountViews = countViews;
		}
		@Override
		public String toString() {
			return PageTitle + '\t' + CountViews;
		}
	}
	
	//domain name
	public final String VALID_DOMAN_REGEX = "\\b(en\\.m)\\b|\\b(en)\\b";
	//title
	public String INVALID_NAMESPACE_REGEX = ""; // generate in function
	public String MEDIA_FILE_REGEX = ""; // generate in function
	public String BOILERPLATE_PAGES_REGEX = "^(404_error/|Main_Page|Hypertext_Transfer_Protocol|Search)";
	public String INVALID_TITLE_REGEX = ""; // generate in function
	
	protected Scanner scanner = null;
	
	protected void GenerateMediaRegex() {
		// URL u = FileFilter.class.getClassLoader().getResource("/Files/MediaExt");
		//URL url = getClass().getClassLoader().getResource("./");
		//System.out.println(url.toString());
		InputStream iStream = this.getClass().getResourceAsStream("/res/MediaExt");
		Scanner scanner = new Scanner(iStream);
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(".*");
		boolean isNotFirstLine = false;
		while (scanner.hasNextLine()) {
			if (isNotFirstLine) {
				stringBuilder.append("|");
			}
			else {
				isNotFirstLine = true;
				stringBuilder.append("(");
			}
			String line = scanner.nextLine();
			stringBuilder.append("\\");
			for (int i = 0; i < line.length(); ++i) {
				char c = line.charAt(i);
				if (c == '.') {
					stringBuilder.append(c);
				}
				else {
					stringBuilder.append(String.format("[%c%c]", (char)(c - 32), c)); // lowser case to upper case
				}
			}
		}
		stringBuilder.append(")$");
		this.MEDIA_FILE_REGEX = stringBuilder.toString();
		scanner.close();
		return;
	}

	protected void GenerateInvalidNamespaceRegex() {
		InputStream iStream = this.getClass().getResourceAsStream("/res/InvalidNamespacesRegex");
		Scanner scanner = new Scanner(iStream);
		while (scanner.hasNextLine()) {
			this.INVALID_NAMESPACE_REGEX = scanner.nextLine();
		}
		scanner.close();
		return;
	}
	
	public FileFilter(String filepath) {
		GenerateMediaRegex();
		GenerateInvalidNamespaceRegex();
		INVALID_TITLE_REGEX = String.format("(%s)|(%s)|(%s)", this.INVALID_NAMESPACE_REGEX, this.BOILERPLATE_PAGES_REGEX, this.MEDIA_FILE_REGEX);
		try {
			scanner = new Scanner(new File(filepath), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			scanner = null;
			return;
		}
		
	}
	
	public void WorkAndWriteResult(OutputStream outputStream) {
		//System.out.println("Start working");
		if (outputStream == null) return;
		if (scanner == null) return;
		ArrayList<FilteredItem> filteredItems = new ArrayList<>();
		
		/*
		File file = new File("/home/qichen/Documents/mid");
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(file);
		}
		catch (Exception e) {
			//e.printStackTrace();
		}*/
		
		
		while (scanner.hasNextLine()) {
			String[] items = scanner.nextLine().split("\\s+");
			//System.out.println("Readline!");
			// Rule 1 dirty row
			if (items.length != 4) continue;
			// Rule 2 English page
			if (!items[0].matches(VALID_DOMAN_REGEX)) continue;
			// Rule 5 Article title limitation
			//if ((items[1].charAt(0) >= 97) && (items[1].charAt(0) <= 122)) continue;
			if (items[1].matches("^[a-z].*")) continue;
			// Rule 3 Invalid namespaces
			// Rule 4 Normalization
			// Rule 6 No media file
			// Rule 7 Boilerplate pages
			if (items[1].matches(this.INVALID_TITLE_REGEX)) continue;
			//System.out.println("Valid Item!");
			
			/*
			try {
				if (items[0].equals("en.m"))
				fileWriter.write(String.format("%s\t%s\t%s\t%s\n", items[0], items[1], items[2], items[3]));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			
			filteredItems.add(new FilteredItem(items[1], Integer.valueOf(items[2])));
		}
		
		// merge the same page_title
		filteredItems.sort(new Comparator<FilteredItem>() {
			@Override
			public int compare(FilteredItem a, FilteredItem b) {
				return a.PageTitle.compareTo(b.PageTitle);
			}
		});
		ArrayList<FilteredItem> mergedList = new ArrayList<>();
		FilteredItem curItem = null;
		for (FilteredItem filteredItem : filteredItems) {
			if (curItem == null) {
				curItem = filteredItem;
			}
			else {
				if (curItem.PageTitle.equals(filteredItem.PageTitle)) {
					curItem.CountViews += filteredItem.CountViews;
				}
				else {
					mergedList.add(curItem);
					curItem = filteredItem;
				}
			}
		}
		mergedList.add(curItem);
		
		// Rule 8: Descending numerical order of number of accesses.
		// 	Break ties with ascending order lexicographical order		
		mergedList.sort(new Comparator<FilteredItem>() {

			@Override
			public int compare(FilteredItem o1, FilteredItem o2) {
				if (o1.CountViews == o2.CountViews) {
					return o1.PageTitle.compareTo(o2.PageTitle);
				}
				else {
					return o2.CountViews - o1.CountViews;
				}
			}
		});
		
		//System.out.println("Start writing");
		OutputStreamWriter writer = new OutputStreamWriter(outputStream);
		try {
			for (FilteredItem filteredItem : mergedList) {
				writer.write(filteredItem.toString());
				writer.write("\n");
			}
			writer.flush();
			writer.close();
			//System.out.println("Finish!");
			return;
		}
		catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	
	
	/*
	 * args[0]: the input file that needed to be filtered. if null, the prog will read file in ./pagecounts-20160501-000000
	 * args[1]: output file. if null, the prog will write the result in the ./output
	 */
	public static void main(String args[]) throws IOException {
		FileFilter filter = null;
		if (args.length == 0) {
			filter = new FileFilter("./pagecounts-20160501-000000");
		}
		else {
			filter = new FileFilter(args[0]);
		}
		
		if (args.length < 2) {
			File output = new File("./output");
			output.createNewFile();
			filter.WorkAndWriteResult(new FileOutputStream(output));
		}
		else {
			try {
				filter.WorkAndWriteResult(new FileOutputStream(new File(args[1])));
			} catch (FileNotFoundException e) {
				//e.printStackTrace();
				//System.out.println(String.format("File: %s is not found", args[1]));
				return;
			}
		}
		
		/*
		String string = "laksjdMedia:";
		String string2 = "MEDIA%3a";
		String string3 = "aliruqoiwncksaj.adsads.pNg";
		System.out.println(string.matches("([mM][Ee][Dd][Ii][Aa]|[sS][Pp][Ee][Cc][Ii][Aa][Ll]|[tT][Aa][Ll][Kk]|[uU][Ss][Ee][Rr]|[uU][Ss][Ee][Rr]_[Tt][Aa][Ll][Kk]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]_[Tt][Aa][Ll][Kk]|[fF][Ii][Ll][Ee]|[fF][Ii][Ll][Ee]_[Tt][Aa][Ll][Kk]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]_[Tt][Aa][Ll][Kk]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]_[Tt][Aa][Ll][Kk]|[hH][Ee][Ll][Pp]|[hH][Ee][Ll][Pp]_[Tt][Aa][Ll][Kk]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]_[Tt][Aa][Ll][Kk]|[pP][Oo][Rr][Tt][Aa][Ll]|[pP][Oo][Rr][Tt][Aa][Ll]_[Tt][Aa][Ll][Kk]|[bB][Oo][Oo][Kk]|[bB][Oo][Oo][Kk]_[Tt][Aa][Ll][Kk]|[dD][Rr][Aa][Ff][Tt]|[dD][Rr][Aa][Ff][Tt]_[Tt][Aa][Ll][Kk]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]_[Tt][Aa][Ll][Kk]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]_[Tt][Aa][Ll][Kk]|[mM][Oo][Dd][Uu][Ll][Ee]|[mM][Oo][Dd][Uu][Ll][Ee]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]|[gG][Aa][Dd][Gg][Ee][Tt]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]_[Tt][Aa][Ll][Kk]|[tT][Oo][Pp][Ii][Cc])((:)|(%3[Aa])).*"));
		System.out.println(string2.matches("([mM][Ee][Dd][Ii][Aa]|[sS][Pp][Ee][Cc][Ii][Aa][Ll]|[tT][Aa][Ll][Kk]|[uU][Ss][Ee][Rr]|[uU][Ss][Ee][Rr]_[Tt][Aa][Ll][Kk]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]_[Tt][Aa][Ll][Kk]|[fF][Ii][Ll][Ee]|[fF][Ii][Ll][Ee]_[Tt][Aa][Ll][Kk]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]_[Tt][Aa][Ll][Kk]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]_[Tt][Aa][Ll][Kk]|[hH][Ee][Ll][Pp]|[hH][Ee][Ll][Pp]_[Tt][Aa][Ll][Kk]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]_[Tt][Aa][Ll][Kk]|[pP][Oo][Rr][Tt][Aa][Ll]|[pP][Oo][Rr][Tt][Aa][Ll]_[Tt][Aa][Ll][Kk]|[bB][Oo][Oo][Kk]|[bB][Oo][Oo][Kk]_[Tt][Aa][Ll][Kk]|[dD][Rr][Aa][Ff][Tt]|[dD][Rr][Aa][Ff][Tt]_[Tt][Aa][Ll][Kk]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]_[Tt][Aa][Ll][Kk]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]_[Tt][Aa][Ll][Kk]|[mM][Oo][Dd][Uu][Ll][Ee]|[mM][Oo][Dd][Uu][Ll][Ee]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]|[gG][Aa][Dd][Gg][Ee][Tt]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]_[Tt][Aa][Ll][Kk]|[tT][Oo][Pp][Ii][Cc])((:)|(%3[Aa])).*"));
		System.out.println(string3.matches(".*(\\.[Pp][Nn][Gg]|\\.[Gg][Ii][Ff]|\\.[Jj][Pp][Gg]|\\.[Jj][Pp][Ee][Gg]|\\.[Tt][Ii][Ff][Ff]|\\.[Tt][Ii][Ff]|\\.[Xx][Cc][Ff]|\\.[Mm][Ii][Dd]|\\.[Oo][Gg][Gg]|\\.[Oo][Gg][Vv]|\\.[Ss][Vv][Gg]|\\.[Dd][Jj][Vv][Uu]|\\.[Oo][Gg][Aa]|\\.[Ff][Ll][Aa][Cc]|\\.[Oo][Pp][Uu][Ss]|\\.[Ww][Aa][Vv]|\\.[Ww][Ee][Bb][Mm]|\\.[Ii][Cc][Oo]|\\.[Tt][Xx][Tt])"));
		*/
	}
}
