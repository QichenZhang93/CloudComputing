
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class Phase1 {
	
    private HashSet<String> sharedWords = new HashSet<>();
    private HashSet<TweetRecord> user1Record;
    private HashSet<TweetRecord> user2Record;
    private int number;
    //private Connection connection;
    int similarityScore;
    String teamID = TeamInfo.TEAM_ID;
    String AWS = TeamInfo.AWS_ACCOUNT_ID;
	
	private void Similarity(HashSet<TweetRecord> user1Record, HashSet<TweetRecord> user2Record) throws Exception {
    	HashMap<String, WordCount> BOW = new HashMap<>();
    	
        Iterator<TweetRecord> recordIterator = user1Record.iterator();
        TweetRecord currentRecord;
        // store BOW of user1
        while (recordIterator.hasNext()) {
        	//System.out.println(user1Record.size());
            currentRecord = recordIterator.next();
            String[] splitString = currentRecord.slimText.split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
            	if (splitString[i].length() == 0) continue;
                if (BOW.containsKey(splitString[i].toLowerCase())) {
                    BOW.get(splitString[i].toLowerCase()).count1++;
                } else {
                    BOW.put(splitString[i].toLowerCase(), new WordCount(1, 0));
                }
            }
        }
        
        // store BOW of user2
        recordIterator = user2Record.iterator();
        while (recordIterator.hasNext()) {
            currentRecord = recordIterator.next();
            String[] splitString = currentRecord.slimText.split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
            	if (splitString[i].length() == 0) continue;
                if (BOW.containsKey(splitString[i].toLowerCase())) {
                    BOW.get(splitString[i].toLowerCase()).count2++;
                }
            }
        }
        
        // get share word & compute similarity score
        Iterator<String> mapIterator = BOW.keySet().iterator();
        
        //HashSet<String> share_word = new HashSet<>();
        //System.out.println(BOW.keySet());
        similarityScore = 0;
        while (mapIterator.hasNext()) {
            String currentWord = mapIterator.next();
            WordCount currentCount = BOW.get(currentWord);
            if (currentCount.count2 != 0) {
                similarityScore += currentCount.count1*currentCount.count2;
                sharedWords.add(currentWord);
            }
        }        
        
        return;
    }
	
	private String getTopResult(int n) {

        // get tweet that contains shared word
        List<TweetRecord> topTweet1 = new ArrayList<TweetRecord>();
        Iterator<TweetRecord> recordIterator = user1Record.iterator();
        while (recordIterator.hasNext()) {
        	TweetRecord currentRecord = recordIterator.next();   
            String[] splitString = currentRecord.slimText.toLowerCase().split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
                if (sharedWords.contains(splitString[i])) {
                    topTweet1.add(currentRecord);
                    break;
                }
            }
        }
        
        List<TweetRecord> topTweet2 = new ArrayList<TweetRecord>();
        recordIterator = user2Record.iterator();
        while (recordIterator.hasNext()) {
            TweetRecord currentRecord = recordIterator.next();   
            String[] splitString = currentRecord.slimText.toLowerCase().split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
                if (sharedWords.contains(splitString[i])) {
                    topTweet2.add(currentRecord);
                    break;
                }
            }
        }
        
        // sort
        topTweet1.sort(new Comparator<TweetRecord>() {
            public int compare(TweetRecord o1, TweetRecord o2) {
                if (o1.impactScore==o2.impactScore) {
                    BigInteger ID1 = new BigInteger(o1.tweetID);
                    BigInteger ID2 = new BigInteger(o2.tweetID);
                    return ID1.compareTo(ID2);
                }
                else
                    return (int) (o2.impactScore-o1.impactScore);
                }
        });
        
        topTweet2.sort(new Comparator<TweetRecord>() {
            public int compare(TweetRecord o1, TweetRecord o2) {
                if (o1.impactScore==o2.impactScore) {
                    BigInteger ID1 = new BigInteger(o1.tweetID);
                    BigInteger ID2 = new BigInteger(o2.tweetID);
                    return ID1.compareTo(ID2);
                }
                else
                    return (int) (o2.impactScore-o1.impactScore);
                }
        });
        
        List<String> share_word_list = new ArrayList<String>(sharedWords);
        share_word_list.sort(new Comparator<String>() {

            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        
        // output
        StringBuffer outputBuffer = new StringBuffer();
        outputBuffer.append(teamID).append(",").append(AWS).append("\n").append(similarityScore).append("\n");
        Iterator<String> stringIterator = share_word_list.iterator();
        while (stringIterator.hasNext()) {
        	String string = stringIterator.next();
        	//System.out.println(string);
        	//if (string.length() == 0) continue;
            outputBuffer.append(string);
            if (stringIterator.hasNext()) {
            	outputBuffer.append("\t");
            }
        }
        outputBuffer.append("\n");
        
        recordIterator = topTweet1.iterator();
        int idx = 0;
        while (recordIterator.hasNext()) {
        	if (idx >= n) break;
        	++idx;
        	TweetRecord currentRecord = recordIterator.next();
            outputBuffer.append(currentRecord.impactScore).append(":").append(currentRecord.tweetID).append(":").append(currentRecord.createTime).append(":").append(currentRecord.censorText).append("\n");
        }
        recordIterator = topTweet2.iterator();
        
        idx = 0;
        while (recordIterator.hasNext()) {
        	if (idx >= n) break;
        	++idx;
        	TweetRecord currentRecord = recordIterator.next();
            outputBuffer.append(currentRecord.impactScore).append(":").append(currentRecord.tweetID).append(":").append(currentRecord.createTime).append(":").append(currentRecord.censorText).append("\n");
        }
        //System.out.println(outputBuffer.toString());
        return outputBuffer.toString();
    }
	
    public String Solve(HashSet<TweetRecord> user1Record, HashSet<TweetRecord> user2Record, int number) {
    	//System.out.println(number);
    	this.user1Record = user1Record;
    	this.user2Record = user2Record;
    	this.number = number;
    	try {
			Similarity(user1Record, user2Record);
			return getTopResult(this.number);
		} catch (Exception e) {
			e.printStackTrace();
			return "Byte";
		}
    }
}