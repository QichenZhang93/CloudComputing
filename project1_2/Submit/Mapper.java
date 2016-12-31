import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

class Filter {
	
	//domain name
	public final String VALID_DOMAN_REGEX = "\\b(en\\.m)\\b|\\b(en)\\b";
	//title
	public String INVALID_NAMESPACE_REGEX = "^([mM][Ee][Dd][Ii][Aa]|[sS][Pp][Ee][Cc][Ii][Aa][Ll]|[tT][Aa][Ll][Kk]|[uU][Ss][Ee][Rr]|[uU][Ss][Ee][Rr]_[Tt][Aa][Ll][Kk]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]|[wW][Ii][Kk][Ii][Pp][Ee][Dd][Ii][Aa]_[Tt][Aa][Ll][Kk]|[fF][Ii][Ll][Ee]|[fF][Ii][Ll][Ee]_[Tt][Aa][Ll][Kk]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]|[mM][Ee][Dd][Ii][Aa][wW][Ii][Kk][Ii]_[Tt][Aa][Ll][Kk]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]|[tT][Ee][Mm][Pp][Ll][Aa][Tt][Ee]_[Tt][Aa][Ll][Kk]|[hH][Ee][Ll][Pp]|[hH][Ee][Ll][Pp]_[Tt][Aa][Ll][Kk]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]|[cC][Aa][Tt][Ee][Gg][Oo][Rr][Yy]_[Tt][Aa][Ll][Kk]|[pP][Oo][Rr][Tt][Aa][Ll]|[pP][Oo][Rr][Tt][Aa][Ll]_[Tt][Aa][Ll][Kk]|[bB][Oo][Oo][Kk]|[bB][Oo][Oo][Kk]_[Tt][Aa][Ll][Kk]|[dD][Rr][Aa][Ff][Tt]|[dD][Rr][Aa][Ff][Tt]_[Tt][Aa][Ll][Kk]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]|[eE][Dd][Uu][Cc][Aa][Tt][Ii][Oo][Nn]_[pP][Rr][Oo][Gg][Rr][Aa][Mm]_[Tt][Aa][Ll][Kk]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]|[tT][Ii][Mm][Ee][Dd][tT][Ee][Xx][Tt]_[Tt][Aa][Ll][Kk]|[mM][Oo][Dd][Uu][Ll][Ee]|[mM][Oo][Dd][Uu][Ll][Ee]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]|[gG][Aa][Dd][Gg][Ee][Tt]_[Tt][Aa][Ll][Kk]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]|[gG][Aa][Dd][Gg][Ee][Tt]_[Dd][Ee][Ff][Ii][Nn][Ii][Tt][Ii][Oo][Nn]_[Tt][Aa][Ll][Kk]|[tT][Oo][Pp][Ii][Cc])((:)|(%3[Aa])).*";
	public String MEDIA_FILE_REGEX = ".*(\\.[Pp][Nn][Gg]|\\.[Gg][Ii][Ff]|\\.[Jj][Pp][Gg]|\\.[Jj][Pp][Ee][Gg]|\\.[Tt][Ii][Ff][Ff]|\\.[Tt][Ii][Ff]|\\.[Xx][Cc][Ff]|\\.[Mm][Ii][Dd]|\\.[Oo][Gg][Gg]|\\.[Oo][Gg][Vv]|\\.[Ss][Vv][Gg]|\\.[Dd][Jj][Vv][Uu]|\\.[Oo][Gg][Aa]|\\.[Ff][Ll][Aa][Cc]|\\.[Oo][Pp][Uu][Ss]|\\.[Ww][Aa][Vv]|\\.[Ww][Ee][Bb][Mm]|\\.[Ii][Cc][Oo]|\\.[Tt][Xx][Tt])$"; // generate in function
	public String BOILERPLATE_PAGES_REGEX = "^(404_error/|Main_Page|Hypertext_Transfer_Protocol|Search)";
	public String INVALID_TITLE_REGEX = ""; // generate in function

	public Filter() {
		INVALID_TITLE_REGEX = String.format("(%s)|(%s)|(%s)", this.INVALID_NAMESPACE_REGEX, this.BOILERPLATE_PAGES_REGEX, this.MEDIA_FILE_REGEX);
	}
	
	/*
	 * To determine whether a string is valid entry
	 * Params:
	 * string: the string that needs to be decided. Encoding should be "UTF-8".
	 * Return: 
	 * true if the string 1) obey all rules in project 1.1 2) has integer in column 2 and 3 (counting from 0).
	 */
	public boolean IsValid(String string) {
		String[] items = string.trim().split("\\s+");
		// Rule 1 dirty row
		if (items.length != 4) return false;;
		// Rule 5 Article title limitation
		if ((items[1].charAt(0) >= 'a') && (items[1].charAt(0) <= 'z')) return false;
		// Number must be number!!
		try {
			if (Integer.parseInt(items[2]) < 0) return false;
			if (Integer.parseInt(items[3]) < 0) return false;
		}
		catch (NumberFormatException e) {
			return false;
		}
		// Rule 2 English page
		if (!items[0].matches(VALID_DOMAN_REGEX)) return false;
		//if (items[1].matches("^[a-z].*")) continue;
		// Rule 3 Invalid namespaces
		// Rule 4 Normalization
		// Rule 6 No media file
		// Rule 7 Boilerplate pages
		if (items[1].matches(this.INVALID_TITLE_REGEX)) return false;
		return true;
	}
}


public class Mapper {
	
	public BufferedReader reader = null;
	public BufferedWriter writer = null;
	
	public Mapper(InputStream in, OutputStream out) throws UnsupportedEncodingException {
		reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
	}
	
	/*
	 * Deal with data and make sure that ALL DATA are clean and valid.
	 */
	public int Map() {
		try {
			String input = null;
			Filter filter = new Filter();
			String filename = null;
			String articleName = null;
			String viewsCount = null;
			int index;
			
			int n = 0;
			
			
			String date;
			while ((input = reader.readLine()) != null) {
				
				
				/*if (n < 8781806) {
					filename = "pagecount-20160501-000000";
					++n;
				}
				else {
					filename = "pagecount-20160502-000000";
				}*/
				filename = System.getenv("mapreduce_map_input_file"); // TODO:pagecounts-yyyymmdd-hh0000/projectcounts-yyyymmdd-hh0000
				
				try {
					if (!filter.IsValid(input)) continue;
					String trimedFilename = filename.trim();
					String[] nameItems = trimedFilename.split("-");
					//date = nameItems[2]; // extract date from filename. the hour part is no longer needed TODO: Here is the problem
					date = nameItems[nameItems.length - 2]; // extract date from filename. the hour part is no longer needed
					Integer.parseInt(date);
					String[] items = input.trim().split("\\s+");
					articleName = items[1];
					//articleName = articleName + "_" + date; // embed the date in article name so that Reducer can extract it
					viewsCount = items[2];
				}
				catch (NumberFormatException e) {
					continue;
				}
				catch (IndexOutOfBoundsException e) { // Catch the exception when the filename is illegal
					continue;
				}
				writer.write(articleName + '\t' + date + '\t' + viewsCount + "\n");
				writer.flush();
			}
		}
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return -1;
		}
		catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, FileNotFoundException {
		Mapper mapper = new Mapper(System.in, System.out); // TODO: Uncomment before running on server
		//Mapper mapper = new Mapper(new FileInputStream("/home/qichen/workspace/project1_2_15619/src/testFile"), System.out);
		mapper.Map();
		System.exit(0);
	}
}