package ccteam.phase1;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;


public class MySQLTask {
    
    //HashSet<String> banned_word;
    private HashMap<String, wordCount> BOW;
    private HashSet<String> share_word;
    private HashSet<tweetRecord> record1;
    private HashSet<tweetRecord> record2;
    private Connection connection;
    int similarityScore;
    // TODO: change
    String teamID = TeamInfo.TEAM_ID;
    String AWS = TeamInfo.AWS_ACCOUNT_ID;

    public String OutputResult(String user1, String user2, int n) {
        try {
            // Initialization
            Initialization();
                        
            // Load data from MySQL/HBase
            Query(user1, user2);

            // Similarity code
            similarityScore = Similarity();
            
            // Output
            return getTopResult(n);
        } catch (Exception e) {
            e.printStackTrace();
            return "Exception occured";
        }
    }
    
    private void Initialization() throws Exception {       
        // initialize shareWordStorage
        BOW = new HashMap<String, wordCount>();
        record1 = new HashSet<tweetRecord>();
        record2 = new HashSet<tweetRecord>();
        share_word = new HashSet<String>();
        
        
        // initialize MySQL
        /*final String username = System.getenv("sqlun");
        final String password = System.getenv("sqlpw");
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/ccteam?useSSL=false", username, password);  */
        connection = MySQLConncetionPool.getConnection();
    }
    
    
    private void Query(String user1, String user2) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("select tid,uid,created_at,impact_score, censored_text,slim_text from twitter where uid = ? or uid = ?");
        preparedStatement.setString(1, user1);
        preparedStatement.setString(2, user2);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String tweet = resultSet.getString(1);
            String user = resultSet.getString(2);
            int impact = resultSet.getInt(4);
            String created_at = resultSet.getString(3);
            String slimText = resultSet.getString(6);
            String censor = resultSet.getString(5);
            if (user.equals(user1)) {
                record1.add(new tweetRecord(tweet, impact, created_at, slimText, censor));
            } else {
                record2.add(new tweetRecord(tweet, impact, created_at, slimText, censor));
            }
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
    
    private int Similarity() throws Exception {
        Iterator<tweetRecord> recordIterator = record1.iterator();
        tweetRecord currentRecord;
        // store BOW of user1
        while (recordIterator.hasNext()) {
            currentRecord = recordIterator.next();
            String[] splitString = currentRecord.tweetText.split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
            	if (splitString[i].length() == 0) continue;
                if (BOW.containsKey(splitString[i].toLowerCase())) {
                    BOW.get(splitString[i].toLowerCase()).count1++;
                } else {
                    BOW.put(splitString[i].toLowerCase(), new wordCount(1, 0));
                }
            }
        }
        
        // store BOW of user2
        recordIterator = record2.iterator();
        while (recordIterator.hasNext()) {
            currentRecord = recordIterator.next();
            String[] splitString = currentRecord.tweetText.split("[^a-zA-Z0-9]+");
            for (int i = 0; i < splitString.length; i++) {
            	if (splitString[i].length() == 0) continue;
                if (BOW.containsKey(splitString[i].toLowerCase())) {
                    BOW.get(splitString[i].toLowerCase()).count2++;
                }
            }
        }
        
        // get share word & compute similarity score
        Iterator<String> mapIterator = BOW.keySet().iterator();
        //System.out.println(BOW.keySet());
        int similarityCode = 0;
        while (mapIterator.hasNext()) {
            String currentWord = mapIterator.next();
            wordCount currentCount = BOW.get(currentWord);
            if (currentCount.count2 != 0) {
                similarityCode += currentCount.count1*currentCount.count2;
                share_word.add(currentWord);
            }
        }        
        
        return similarityCode;
    }
    
    public String getTopResult(int n) {
        // get regex
        StringBuffer buffer = new StringBuffer("((.*[^a-zA-Z0-9])|^)(?i:");
        Iterator<String> stringIterator = share_word.iterator();
        while (stringIterator.hasNext()) {
            buffer.append(stringIterator.next());
            if (stringIterator.hasNext()) {
                buffer.append("|");
            }
        }
        buffer.append(")($|([^a-zA-Z0-9].*))");
        String regex = buffer.toString();
        
        // get tweet that contains shared word
        List<tweetRecord> topTweet1 = new ArrayList<tweetRecord>();
        Iterator<tweetRecord> recordIterator = record1.iterator();
        while (recordIterator.hasNext()) {
            tweetRecord currentRecord = recordIterator.next();            
            if (currentRecord.tweetText.matches(regex)) {
                topTweet1.add(currentRecord);
            }
        }
        
        List<tweetRecord> topTweet2 = new ArrayList<tweetRecord>();
        recordIterator = record2.iterator();
        while (recordIterator.hasNext()) {
            tweetRecord currentRecord = recordIterator.next();            
            if (currentRecord.tweetText.matches(regex)) {
                topTweet2.add(currentRecord);
            }
        }
        
        // sort
        topTweet1.sort(new Comparator<tweetRecord>() {
            public int compare(tweetRecord o1, tweetRecord o2) {
                if (o1.impactScore==o2.impactScore) {
                    BigInteger ID1 = new BigInteger(o1.tweetID);
                    BigInteger ID2 = new BigInteger(o2.tweetID);
                    return ID1.compareTo(ID2);
                }
                else
                    return (o2.impactScore-o1.impactScore);
                }
        });
        
        topTweet2.sort(new Comparator<tweetRecord>() {
            public int compare(tweetRecord o1, tweetRecord o2) {
                if (o1.impactScore==o2.impactScore) {
                    BigInteger ID1 = new BigInteger(o1.tweetID);
                    BigInteger ID2 = new BigInteger(o2.tweetID);
                    return ID1.compareTo(ID2);
                }
                else
                    return (o2.impactScore-o1.impactScore);
                }
        });
        
        List<String> share_word_list = new ArrayList<String>(share_word);
        share_word_list.sort(new Comparator<String>() {

            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        
        // output
        StringBuffer outputBuffer = new StringBuffer();
        outputBuffer.append(teamID).append(",").append(AWS).append("\n").append(similarityScore).append("\n");
        stringIterator = share_word_list.iterator();
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
            tweetRecord currentRecord = recordIterator.next();
            outputBuffer.append(currentRecord.impactScore).append(":").append(currentRecord.tweetID).append(":").append(currentRecord.createTime).append(":").append(currentRecord.censorText).append("\n");
        }
        recordIterator = topTweet2.iterator();
        
        idx = 0;
        while (recordIterator.hasNext()) {
        	if (idx >= n) break;
        	++idx;
            tweetRecord currentRecord = recordIterator.next();
            outputBuffer.append(currentRecord.impactScore).append(":").append(currentRecord.tweetID).append(":").append(currentRecord.createTime).append(":").append(currentRecord.censorText).append("\n");
        }
        //System.out.println(outputBuffer.toString());
        return outputBuffer.toString();
    }
    
    

    class wordCount {
        int count1;
        int count2;
        
        public wordCount(int num1, int num2) {
            count1 = num1;
            count2 = num2;
        }
    }
    
    class tweetRecord {
        String tweetID;
        int impactScore;
        String createTime;
        String tweetText;
        String censorText;
        
        public tweetRecord(String tweet, int impact, String time, String text, String censor) {
            tweetID = tweet;
            impactScore = impact;
            createTime = time;
            tweetText = text;
            censorText = censor;
        }
    }
}