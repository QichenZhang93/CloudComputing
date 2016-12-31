

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class Tools {
	public static void Log(String info) {
		System.out.println(info);
	}
	
	public static String GetPittsburgTime() {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("GMT-4"));
		String time = format.format(calendar.getTime());
		return time;
	}
	
	public static BigInteger LongToBigInt(long l) {
		long l3 = l;
		long l2 = l3 & Long.MIN_VALUE; // get MSB
		l3 &= (Long.MIN_VALUE - 1); // remove MSB
		BigInteger bigInteger = BigInteger.valueOf(l3);
		if (l2 != 0) {
			bigInteger = bigInteger.setBit(63);
		}
		return bigInteger;
	}
	
	/**
	 * make "\\n" to "\n" ... TODO: more to come
	 * @param text
	 * @return
	 */
	public static String turnEscape(String text) {
		text = text
				.replaceAll("\\t", "\t")
				.replaceAll("\\n", "\n")
				.replaceAll("\\r", "\r")
				.replaceAll("\\b", "\b")
				.replaceAll("\\f", "\f")
				.replaceAll("\\u0085", "\u0085")
				.replaceAll("\\u001c", "\u001c")
				.replaceAll("\\u001d", "\u001d")
				.replaceAll("\\u001e", "\u001e")
				.replaceAll("\\u001f", "\u001f")
				.replaceAll("\\u000a", "\n")
				.replaceAll("\\u000b", "\u000b")
				.replaceAll("\\u000d", "\r");
		return text;
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
	
	/**
	 * extend a string representation of a long to 20 characters
	 * Long.MAX = 18446744073709551615
	 * @param longInString
	 * @return
	 */
	public static String unsignedExtendLong(String longInString) {
		char[] zeros = new char[20];
		for (int i = 0; i < 20; ++i)
			zeros[i] = '0';
		return new String(zeros, 0, 20 - longInString.length()) + longInString;
	}
	
	public static void main(String[] args) {
		
	}
}
