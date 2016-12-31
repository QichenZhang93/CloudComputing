import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;

public class Phase2 {
	
	private static final HashSet<String> STOP_WORDS = new HashSet<>();
	private static final HashSet<String> BANNED_WORDS = new HashSet<>();
	
	private static boolean isInit = false;
	
	private static void init() {
		try {
			InputStreamReader reader = new InputStreamReader(Phase2.class.getResourceAsStream("./bannedwords.txt"), "UTF-8");
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				Phase2.BANNED_WORDS.add(line.trim());
			}
			bufferedReader.close();
			reader.close();
			
			reader = new InputStreamReader(Phase2.class.getResourceAsStream("./stopwords.txt"), "UTF-8");
			bufferedReader = new BufferedReader(reader);
			while ((line = bufferedReader.readLine()) != null) {
				Phase2.STOP_WORDS.add(line);
			}
			bufferedReader.close();
			reader.close();
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
	}
	
	/**
	 * 
	 * @param prefixes prefixes
	 * @param nums number of occurance of each prefix
	 * @param words words extracted from slim_text
	 */
	public static void SolveOneRow(String[] prefixes, int[] nums, String[] words) {
		if (!isInit) {
			init();
			isInit = true;
		}
		for (String word : words) {
			for(int i = 0; i < prefixes.length; ++i) {
				if (BANNED_WORDS.contains(word) || STOP_WORDS.contains(word)) {
					continue;
				}
				if (word.matches("^" + prefixes[i] + ".*")) {
					
					++ nums[i];
				}
			}
		}
	}
	
	public static void main(String[] args) {
		String[] pre = new String[] {"nobhe"};
		String[] words = new String[] { "nobhead", "along", "alo", "sl", "", "distance0", "09dirr", "fuck", "Shit", "lon", "pre" };
		int[] n = new int[1];
		n[0] = 0;
		SolveOneRow(pre, n, words);
		for (int i : n) {
			System.out.println(i);
		}
	}
}
