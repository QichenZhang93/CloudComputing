
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableMap;

public class HBaseTasks {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.58.81"; // TODO
    /**
     * The name of your HBase table.
     */
    private static final TableName bQ2TableName = TableName.valueOf("tweetTable");
    private static final TableName bQ3TableName = TableName.valueOf("q3tweet");
    

    /**
     * HBase connection.
     */
    private static Connection conn;
    /**
     * Byte representation of column family.
     */
    private static final byte[] bTweetColFamilyName = Bytes.toBytes("tweet");
    private static final byte[] bColName = Bytes.toBytes("NB");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Initialize HBase connection.
     * @throws IOException
     */
    public static void initializeConnection() throws IOException {
        LOGGER.setLevel(Level.ERROR);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conf.set("hbase.client.localityCheck.threadPoolSize", "2");
        conf.set("hbase.thrift.minWorkerThreads", "500");
        
        conn = ConnectionFactory.createConnection(conf);
    }
    
    private HashSet<TweetRecord> user1Record = new HashSet<>();
    private HashSet<TweetRecord> user2Record = new HashSet<>();
    
    int similarityScore;
    String teamID = TeamInfo.TEAM_ID;
    String AWS = TeamInfo.AWS_ACCOUNT_ID;
    
    public String ExecuteQuery3(Long uid_start, Long uid_end, Long tid_start, Long tid_end, String date_start, String date_end, String[] prefixes) {

    	long l_conn = System.currentTimeMillis();
    	
    	try (Table q3Table = conn.getTable(bQ3TableName)) {
    		
    		long l_query = System.currentTimeMillis();
    		
    		uid_end += 1; // exclusive
    		
    		Scan scan = new Scan(uid_start.toString().getBytes(), uid_end.toString().getBytes());
    		// scan.setCaching(300);

    		ResultScanner scanner = q3Table.getScanner(scan);
    		
    		long l_algo = System.currentTimeMillis();
    		
    		// --- counting prefixes
    		int[] num_prefixes = new int[prefixes.length];
			for (int i = 0; i < num_prefixes.length; ++i) {
				num_prefixes[i] = 0;
			}
			// ---
			Result result = null;
			while ((result = scanner.next()) != null) {

    			String tweetsInfo = new String(result.getValue(bTweetColFamilyName, bColName), "UTF-8");
    			
    			Long thisUid = Long.valueOf(Bytes.toString(result.getRow()));
    			if (thisUid > uid_end || thisUid < uid_start) {
    				continue;
    			}
    			
    			String[] singleTweets = tweetsInfo.split("\\t+");
    			if (singleTweets.length == 0) {
    				continue;
    			}
    			
    			for (String singleTweet : singleTweets) {
    				
    				if (singleTweet.length() == 0) continue;
					String[] infoPieces = singleTweet.split("\\s+");
					Long tid = Long.valueOf(infoPieces[0]);
	    			if (tid < tid_start || tid > tid_end) {
						continue;
					}
	    			String created_at = infoPieces[1];
					if (created_at.compareTo(date_start) < 0 || created_at.compareTo(date_end) > 0) {
						continue;
					}
					for (int i = 2; i < infoPieces.length; ++i) {
	    				String word = infoPieces[i];
	    				for (int j = 0; j < prefixes.length; ++j) {
							if (word.startsWith(prefixes[j])) {
								num_prefixes[j] += 1;
							}
						}
	    			}
				}
			}
			try {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(TeamInfo.TEAM_ID).append(TeamInfo.AWS_ACCOUNT_ID).append("\n");
				for (int i = 0; i < prefixes.length; ++i) {
					stringBuilder.append(prefixes[i])
						.append(":")
						.append(num_prefixes[i])
						.append("\n");
				}
				long l_fin = System.currentTimeMillis();
				System.out.println(String.format("CONN %s QUERY %s ALGO %s", l_query - l_conn, l_algo - l_query, l_fin - l_algo));
				return stringBuilder.toString();
			} catch (Exception e) {
				e.printStackTrace();
			}
    	} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
    	return "";
    }
    
    public String ExecuteQuery2(String userid1, String userid2, int number) {
    	
    	long l_conn = System.currentTimeMillis();
    	
    	try (Table userTweetTable = conn.getTable(bQ2TableName)) {
    		ArrayList<Get> getlist = new ArrayList<>();
    		Get getUserId1 = new Get(userid1.getBytes());
    		getUserId1.addFamily(bTweetColFamilyName);
        	Get getUserId2 = new Get(userid2.getBytes());
        	getUserId2.addFamily(bTweetColFamilyName);
        	getlist.add(getUserId1);
        	getlist.add(getUserId2);
        	
        	long l_query = System.currentTimeMillis();
        	
    		Result[] results = userTweetTable.get(getlist);
    		userTweetTable.close();
    		
    		long l_algo = System.currentTimeMillis();
    		
    		NavigableMap<byte[], byte[]> user1TweetId2Info = results[0].getFamilyMap(bTweetColFamilyName);
    		String tweetInfo = new String(user1TweetId2Info.get(bColName), "UTF-8");
            String[] tweetSmallInfo = tweetInfo.split("\\t+");
            if (tweetSmallInfo.length == 0 || tweetSmallInfo.length % 5 != 0) {
                return "user1 tweet length not match";
            }
            for (int i = 0; i + 5 <= tweetSmallInfo.length; i += 5) {
            	String str1 = tweetSmallInfo[i+4].replaceAll("u0085", "\u0085");
            	String str2 = tweetSmallInfo[i+3].replaceAll("u0085", "\u0085");
                user1Record.add(new TweetRecord(
                        tweetSmallInfo[i],
                        Integer.valueOf(tweetSmallInfo[i+2]),
                        tweetSmallInfo[i+1],
                        str1,
                        str2));
            }
    		
    		NavigableMap<byte[], byte[]> user2TweetId2Info = results[1].getFamilyMap(bTweetColFamilyName);
            tweetInfo = new String(user2TweetId2Info.get(bColName), "UTF-8");
            tweetSmallInfo = tweetInfo.split("\\t+");
            if (tweetSmallInfo.length == 0 || tweetSmallInfo.length % 5 != 0) {
                return "user2 tweet length not match";
            }
            for (int i = 0; i + 5 <= tweetSmallInfo.length; i += 5) {
            	String str1 = tweetSmallInfo[i+4].replaceAll("u0085", "\u0085");
            	String str2 = tweetSmallInfo[i+3].replaceAll("u0085", "\u0085");
                user2Record.add(new TweetRecord(
                        tweetSmallInfo[i],
                        Integer.valueOf(tweetSmallInfo[i+2]),
                        tweetSmallInfo[i+1],
                        str1,
                        str2
                        ));
            }
            
            long l_r = System.currentTimeMillis();
    		
    		String resultString = new Phase1().Solve(user1Record, user2Record, number);
    		long l_fin = System.currentTimeMillis();
			System.out.println(String.format("CONN %s QUERY %s LOAD %s ALGO %s", l_query - l_conn, l_algo - l_query, l_r - l_algo, l_fin - l_r));
    		return resultString;
    		
    	} catch (IOException e) {
			e.printStackTrace();
			return "Bye";
		}
    }
}