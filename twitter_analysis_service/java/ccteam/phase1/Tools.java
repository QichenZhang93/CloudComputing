package ccteam.phase1;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import ccteam.phase1.etl.BriefTweet;

public class Tools {
	public static String GetPittsburgTime() {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("GMT-4"));
		String time = format.format(calendar.getTime());
		return time;
	}
	
	public static String escapeSpecialCharForReducer(String text) {
		String string = text;
		string = string/*.replace("\\", "\\\\").replace("\"", "\\\"").replace("\f", "\\f")
				.replace("\t", "\\t").replace("\b", "\\b").replace("\r", "\\r")*/.replace("\n", "\\n");
		return string;
	}
	
	public static String escapeSpecialCharForMysql(String text) {
		String string = text;
		string = string/*.replace("\\", "\\\\")*//*replace("\"", "\\\"")*///.replace("\f", "\\f")
				/*.replace("\t", "\\t").replace("\b", "\\b").replace("\r", "\\r")*///.replace("\n", "\\n")
				.replace("'", "\\'").replace("\\n", "\n")/*.replace("%", "\\%").replace("_", "\\_")*/;
		return string;
	}
	
	// Phase 1 Q2
	public static boolean validateUserId(String userid1, String userid2) {
		if (userid1 == null || userid1.length() == 0 || userid2 == null || userid2.length() == 0) {
			return false;
		}
		try {
			Long.valueOf(userid1);
			Long.valueOf(userid2);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	
	public static void main(String[] args) {
		String string = "\\";
		String string2 = string + "n";
		System.out.println(string2);
		String string3 = string2;
		System.out.println(string3);
	}
	
	
}
