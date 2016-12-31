package ccteam.phase1.etl;

import java.io.InputStream;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import ccteam.phase1.Tools;

public class BriefTweet {
	// ids are BitInteger because id_str represents a id that's larger than int64 can represent
	public BigInteger tweetId = null; // should be unsigned int 64 sort in numerical order
	public BigInteger userId = null; // should be unsigned int64
	
	public String createdAt = null;
	public String text = null;
	public String slimText = null; // text after removing short URL
	public String censoredText = null;
	public Long impactScore = 0L; // should be unsigned int 64
	public Integer favorite_count = 0;
	public Integer retweet_count = 0;
	public Integer followers_count = 0;
	
	private static final String WORD_PATTERN = "[a-zA-Z0-9]+";
	private static final String NOT_WORD_PATTERN = "[^a-zA-Z0-9]+";
	private static AtomicBoolean isFileLoaded = new AtomicBoolean(false);
	
	private static HashSet<String> stopWordsSet = null;
	private static HashMap<String, Long> wordScoreMap = null;
	private static HashSet<String> bannedWordsSet = null;
	
	private static void readBannedWords() {
		if (bannedWordsSet != null) return;
		InputStream iStream = BriefTweet.class.getResourceAsStream("bannedWords");
		Scanner scanner = new Scanner(iStream, "UTF-8");
		String word = null;
		bannedWordsSet = new HashSet<String>();
		while (scanner.hasNextLine()) {
			word = scanner.nextLine();
			if (word.length() == 0) continue;
			bannedWordsSet.add(word);
		}
		scanner.close();
	}

	private static void readStopWords() {
		if (stopWordsSet != null) return;
		InputStream iStream = BriefTweet.class.getResourceAsStream("stopwords.txt");
		Scanner scanner = new Scanner(iStream, "UTF-8");
		String word = null;
		stopWordsSet = new HashSet<String>();
		while (scanner.hasNextLine()) {
			word = scanner.nextLine();
			if (word.length() == 0) continue;
			stopWordsSet.add(word);
		}
		scanner.close();
	}

	private static void readSentimentScore() {
		if (wordScoreMap != null) return;
		InputStream iStream = BriefTweet.class.getResourceAsStream("AFINN.txt");
		Scanner scanner = new Scanner(iStream, "UTF-8");
		String lineString = null;
		wordScoreMap = new HashMap<String, Long>();
		
		while (scanner.hasNextLine()) {
			lineString = scanner.nextLine();
			if (lineString == null || lineString.length() == 0) continue;
			String[] elems = lineString.split("\\s+"); // split according to general space (tab included)
			if (elems.length < 2) continue;
			try {
				wordScoreMap.put(elems[0], Long.valueOf(elems[1]));
			} catch (NumberFormatException e) {
				continue;
			}
		}
		scanner.close();
	}

	private void loadData() {
		if (isFileLoaded.get()) return;
		readBannedWords();
		readStopWords();
		readSentimentScore();
		isFileLoaded.set(true);
	}
	
	public BriefTweet() {
		if (!isFileLoaded.get()) {
			loadData();
			isFileLoaded.set(true);
		}
	}
	
	public String makeString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(tweetId).append("\t").append(userId).append("\t").append(createdAt)
			.append("\t").append(impactScore).append("\t")./*append(text).append("\t").*/append(censoredText).append("\t").append(slimText);
		System.out.println(stringBuilder.toString());
		return stringBuilder.toString();
	}
	
	private void convertTimeFormat() {
        String[] splitString = createdAt.split(" ");
        String date = splitString[1]+" "+splitString[2]+" "+splitString[5]+" "+splitString[3];
        SimpleDateFormat format1 = new SimpleDateFormat("MMMM dd yyyy HH:mm:ss",Locale.US);
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        try {
			String formatDate = format2.format(format1.parse(date)).toString();
			createdAt = formatDate;
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	private void censorText() {
		Scanner scanner = new Scanner(text);
		String token = null;
		int idx = 0;
		StringBuilder censoredTextBuilder = new StringBuilder();
		scanner.useDelimiter(NOT_WORD_PATTERN);
		while (scanner.hasNext(WORD_PATTERN)) {
			token = scanner.next(WORD_PATTERN);
			// move idx to token's head
			while (token.charAt(0) != text.charAt(idx)) {
				censoredTextBuilder.append(text.charAt(idx));
				++idx;
			}
				
			if (bannedWordsSet.contains(token.toLowerCase())) {
				int length = token.length() - 2;
				censoredTextBuilder.append(token.charAt(0));
				for (int i = 0; i < length; ++i) {
					censoredTextBuilder.append("*");
				}
				censoredTextBuilder.append(token.charAt(token.length() - 1));
				idx += token.length();
			}
			else {
				censoredTextBuilder.append(token);
				idx += token.length();
			}
		}
		scanner.close();
		while (idx < text.length()) {
			censoredTextBuilder.append(text.charAt(idx));
			++idx;
		}
		this.censoredText = censoredTextBuilder.toString();
	}
	
	private void removeShortURL() {
		slimText = text.replaceAll("(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*", "");
	}
	
	private Long calculateEffectiveWordCount(Set<String> stopwordsSet) {
		Scanner scanner = new Scanner(slimText);
		String token = null;
		Long ewc = 0L;
		scanner.useDelimiter(NOT_WORD_PATTERN);
		while (scanner.hasNext(WORD_PATTERN)) {
			token = scanner.next(WORD_PATTERN).toLowerCase();
			if (!stopwordsSet.contains(token)) {
				ewc += 1;
			}
		}
		scanner.close();
		return ewc;
	}
	
	private Long calculateInfluentialScore(Set<String> stopwordsSet) {
		Long ewc = calculateEffectiveWordCount(stopwordsSet);
		return ewc * (followers_count + favorite_count + retweet_count);
	}
	
	private Long calculateSentimentScore(Map<String, Long> wordScoreMap) {
		Long sentiScore = 0L;
		Scanner scanner = new Scanner(slimText);
		String token = null;
		scanner.useDelimiter(NOT_WORD_PATTERN);
		while (scanner.hasNext(WORD_PATTERN)) {
			token = scanner.next().toLowerCase();
			if (wordScoreMap.containsKey(token)) {
				sentiScore += (wordScoreMap.get(token));
			}
		}
		scanner.close();
		return sentiScore;
	}
	
	private void calculateImpactScore() {
		Long sentiScore = calculateSentimentScore(wordScoreMap);
		Long inflScore = calculateInfluentialScore(stopWordsSet);
		this.impactScore = sentiScore + inflScore;
	}
	
	private void escapeSpecialChar() {
		this.text = Tools.escapeSpecialCharForReducer(this.text);
		this.censoredText = Tools.escapeSpecialCharForReducer(this.censoredText);
		this.slimText = Tools.escapeSpecialCharForReducer(this.slimText);
	}
	
	public void makeThisTweetGood() {
		censorText();
		removeShortURL();
		calculateImpactScore();
		escapeSpecialChar();
		convertTimeFormat();
	}
	
	public boolean isMalformed() {
		// malformed tweet
		if (tweetId == null) {
			return true;
		}
		if (text == null || text.length() == 0) {
			return true;
		}
		if (userId == null) {
			return true;
		}
		if (createdAt == null || createdAt.length() == 0) {
			return true;
		}
		return false;
	}
}