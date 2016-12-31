package ccteam.phase1.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

import ccteam.phase1.Tools;

public class Reducer {
	public static void main(String[] args) {
		BufferedReader inputReader = null;
		try {
			inputReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		String lineString;
		BigInteger previousTweetId = null;
		BigInteger newTweetId = null;
		try {
			PrintStream printStream = new PrintStream(System.out, true, "UTF-8");
			while ((lineString = inputReader.readLine()) != null) {
				// tid \t uid \t createdAt \t impactScore \t censoredText \t slimText
				String[] tweetInfo = lineString.split("\t");
				if (tweetInfo.length != 6) {
					continue;
				}
				else {
					//System.out.println(previousTweetId);
					if (previousTweetId == null) {
						//System.out.println(tweetInfo[0]);
						previousTweetId = new BigInteger(tweetInfo[0]);
					}
					else {
						newTweetId = new BigInteger(tweetInfo[0]);
						//System.out.println(previousTweetId + " " + newTweetId);
						if (previousTweetId.equals(newTweetId)) {
							//System.out.println("EQUALS!!");
							continue;
						}
						else previousTweetId = newTweetId;
					}
					
					
					StringBuilder stringBuilder = new StringBuilder();
					boolean isFirst = true;
					for (String singleTweet : tweetInfo) {
						if (!isFirst) {
							stringBuilder.append("\t");
						}
						isFirst = false;
						stringBuilder.append(Tools.escapeSpecialCharForMysql(singleTweet));
					}
					printStream.println(stringBuilder.toString());
				}
			}
			printStream.close();
			inputReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
