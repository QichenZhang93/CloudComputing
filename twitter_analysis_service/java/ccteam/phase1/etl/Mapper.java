package ccteam.phase1.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import com.google.gson.stream.JsonReader;

import ccteam.phase1.Tools;

class JsonKeyName {
	public static final String CREATED_AT = "created_at"; // String in source data-UTC time
	public static final String ID = "id"; // int64 in source data-for both tweet & users
	public static final String ID_STR = "id_str"; // String in source data-for both tweet & users
	public static final String LANG = "lang"; // String in source data
	public static final String TEXT = "text"; // String in source data-UTF-8
	public static final String USER = "user"; // Users in source data
	// Below three fields are integers in json
	public static final String FAVORITE_COUNT = "favorite_count";
	public static final String RETWEET_COUNT = "retweet_count";
	public static final String FOLLOWERS_COUNT = "followers_count";
}

public class Mapper {

	public static void main(String[] args) {
		
		BufferedReader inputReader = null;
		try {
			inputReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		// Read a line (tweet json object)
		String lineString;
		try {
			PrintStream printStream = new PrintStream(System.out, true, "UTF-8");
			while ((lineString = inputReader.readLine()) != null) {
				try {
				//Tools.Log(lineString);
				// parse json here get id/id_str, uid, text, created_at
				JsonReader jReader = new JsonReader(new StringReader(lineString));
				jReader.beginObject();
				String keyNameInTweet;
				String keyNameInUser = null;
				String stringValue;
				BriefTweet briefTweet = new BriefTweet();
				boolean isEnglishTweet = false;
				while (jReader.hasNext()) {
					keyNameInTweet = jReader.nextName();
					if (keyNameInTweet.equals(JsonKeyName.CREATED_AT)) {
						briefTweet.createdAt = jReader.nextString();
					}
					else if (keyNameInTweet.equals(JsonKeyName.ID)) {
						try {
							briefTweet.tweetId = new BigInteger(String.valueOf(jReader.nextLong()));
						} catch (NumberFormatException e) {
							continue;
						} catch (IllegalStateException e) {
							continue;
						}
					}
					else if (keyNameInTweet.equals(JsonKeyName.ID_STR)) {
						try {
							briefTweet.tweetId = new BigInteger(jReader.nextString());
						} catch (NumberFormatException e) {
							continue;
						} catch (IllegalStateException e) {
							continue;
						}
					}
					else if (keyNameInTweet.equals(JsonKeyName.LANG)) {
						try {
							stringValue = jReader.nextString();
							if (!stringValue.equals("en")) {
								break; // if lang != "en", skip this line
							}
							isEnglishTweet = true;
						} catch (IllegalStateException e) {
							// next token is not a literal value
							break;
						}
					}
					else if (keyNameInTweet.equals(JsonKeyName.TEXT)) {	
						try {
							briefTweet.text = jReader.nextString();
						} catch (IllegalStateException e) {
							// next token is not a literal value
							continue;
						}
					}
					else if (keyNameInTweet.equals(JsonKeyName.USER)) {
						// parse user
						jReader.beginObject();
						keyNameInUser = null;
						while (jReader.hasNext()) {
							keyNameInUser = jReader.nextName();
							if (keyNameInUser.equals(JsonKeyName.ID)) {
								try {
									briefTweet.userId = new BigInteger(String.valueOf(jReader.nextLong()));
									continue;
								} catch (NumberFormatException e) {
									// id is illegal. wait for id_str
									continue;
								} catch (IllegalStateException e) {
									// next token is not a literal value
									continue;
								}
							}
							else if (keyNameInUser.equals(JsonKeyName.ID_STR)) {
								try {
									briefTweet.userId = new BigInteger(jReader.nextString());
								} catch (NumberFormatException e) {
									// id is illegal. wait for id
									continue;
								} catch (IllegalStateException e) {
									// next token is not a literal value
									continue;
								}
							}
							else if (keyNameInUser.equals(JsonKeyName.FOLLOWERS_COUNT)) {	
								try {
									briefTweet.followers_count = jReader.nextInt();
								} catch (NumberFormatException e) {
									// id is illegal. wait for id
									continue;
								} catch (IllegalStateException e) {
									// next token is not a literal value
									continue;
								}
							}
							else {
								jReader.skipValue();
							}
						}
						jReader.endObject();
					}
					else if (keyNameInTweet.equals(JsonKeyName.FAVORITE_COUNT)) {
						try {
							briefTweet.favorite_count = jReader.nextInt();
						} catch (NumberFormatException e) {
							// id is illegal. wait for id
							continue;
						} catch (IllegalStateException e) {
							// next token is not a literal value
							continue;
						}
					}
					else if (keyNameInTweet.equals(JsonKeyName.RETWEET_COUNT)) {	
						try {
							briefTweet.retweet_count = jReader.nextInt();
						} catch (NumberFormatException e) {
							// id is illegal. wait for id
							continue;
						} catch (IllegalStateException e) {
							// next token is not a literal value
							continue;
						}
					}
					else {
						try {
							jReader.skipValue();
						} catch (Exception e) {
							continue;
						}
					}
				
				}
				jReader.endObject();
				jReader.close();
				if (briefTweet.isMalformed()) continue;
				if (!isEnglishTweet) continue; // skip this line if it's not an English tweet
				// cut word in "text" into space separated-->no need! in twitter, tab is not allowed.
				// generate censored text
				briefTweet.makeThisTweetGood();
				printStream.println(briefTweet.makeString());
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}
			printStream.close();
			inputReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			inputReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
