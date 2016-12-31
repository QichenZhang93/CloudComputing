import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;

class Query2Solution
{
	public int SimilarityScore = 0;
	public List<FullTweet> User1Tweets = new ArrayList<>();
	public List<FullTweet> User2Tweets = new ArrayList<>();
	public List<String> SharedWords = new ArrayList<>();
	
	@Override
	public String toString()
	{
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(SimilarityScore).append("\n");
		boolean isFirst = true;
		for (String sWord : SharedWords)
		{
			if (isFirst)
			{
				isFirst = false;
			}
			else
			{
				sBuilder.append('\t');
			}
			sBuilder.append(sWord);
		}
		sBuilder.append('\n');
		for (FullTweet t : User1Tweets)
		{
			sBuilder.append(t.impactScore).append(':')
				.append(t.tid).append(':')
				.append(t.createdAt).append(':')
				.append(t.censoredText).append('\n');
		}
		for (FullTweet t : User2Tweets)
		{
			sBuilder.append(t.impactScore).append(':')
				.append(t.tid).append(':')
				.append(t.createdAt).append(':')
				.append(t.censoredText).append('\n');
		}
		
		return sBuilder.toString();
	}
	
	private static class WordInfo
	{
		List<Integer> tweetIndex_user1 = new ArrayList<>();
		List<Integer> tweetIndex_user2 = new ArrayList<>();
		Integer occurance_user1 = 0;
		Integer occurance_user2 = 0;
	}
	
	public static Query2Solution GetSolution(final List<FullTweet> user1Tweet, final List<FullTweet> user2Tweet, final int number)
	{
		Query2Solution solution = new Query2Solution();
		HashMap<String, WordInfo> wordsInfo = new HashMap<>();
		
		for (int idx = 0; idx < user1Tweet.size(); ++idx)
		{
			FullTweet tweet = user1Tweet.get(idx);
			for (String word : tweet.wordToCount.keySet())
			{
				WordInfo info = wordsInfo.get(word);
				if (info == null)
				{
					info = new WordInfo();
				}
				info.occurance_user1 += tweet.wordToCount.get(word);
				info.tweetIndex_user1.add(idx);
				wordsInfo.put(word, info);
			}
		}
		for (int idx = 0; idx < user2Tweet.size(); ++idx)
		{
			FullTweet tweet = user2Tweet.get(idx);
			for (String word : tweet.wordToCount.keySet())
			{
				if (wordsInfo.containsKey(word))
				{
					WordInfo info = wordsInfo.get(word);
					info.occurance_user2 += tweet.wordToCount.get(word);
					info.tweetIndex_user2.add(idx);
					wordsInfo.put(word, info);
				}
			}
		}
		
		final HashSet<Integer> IndexOfTweetsContainingSharedWords_user1 = new HashSet<>();
		final HashSet<Integer> IndexOfTweetsContainingSharedWords_user2 = new HashSet<>();
		
		for (Entry<String, WordInfo> entry : wordsInfo.entrySet()) {
			if (entry.getValue().occurance_user1 != 0 && entry.getValue().occurance_user2 != 0)
			{
				solution.SharedWords.add(entry.getKey());
				solution.SimilarityScore += (entry.getValue().occurance_user1 * entry.getValue().occurance_user2);
				for (int idx : entry.getValue().tweetIndex_user1)
				{
					if (IndexOfTweetsContainingSharedWords_user1.contains(idx))
						continue;
					else
					{
						IndexOfTweetsContainingSharedWords_user1.add(idx);
						solution.User1Tweets.add(user1Tweet.get(idx));
					}
					
				}
				for (int idx : entry.getValue().tweetIndex_user2)
				{
					if (IndexOfTweetsContainingSharedWords_user2.contains(idx))
						continue;
					else
					{
						IndexOfTweetsContainingSharedWords_user2.add(idx);
						solution.User2Tweets.add(user2Tweet.get(idx));
					}
				}
			}
		}
		
		solution.User1Tweets.sort((o1, o2) -> {
			if (o1.impactScore.equals(o2.impactScore))
			{
				return o1.tid.compareTo(o2.tid);
			}
			else
			{
				return o2.impactScore.compareTo(o1.impactScore);
			}
		});
		
		if (solution.User1Tweets.size() > number)
			solution.User1Tweets = solution.User1Tweets.subList(0, number);
		
		solution.User2Tweets.sort((o1, o2) -> {
			if (o1.impactScore.equals(o2.impactScore))
			{
				return o1.tid.compareTo(o2.tid);
			}
			else
			{
				return o2.impactScore.compareTo(o1.impactScore);
			}
		});
		
		if (solution.User2Tweets.size() > number)
			solution.User2Tweets = solution.User2Tweets.subList(0, number);
		
		solution.SharedWords.sort((o1, o2) -> {
			return o1.compareTo(o2);
		});
		
		return solution;
	}
}

class FullTweet {
	public Long uid;
	public Long tid;
	public String createdAt;
	public String censoredText;
	public HashMap<String, Integer> wordToCount;
	public Integer impactScore;
	
	public FullTweet(Long u, Long t, String c_time, String c_text, Integer im_score) {
		uid = u;
		tid = t;
		createdAt = c_time;
		censoredText = c_text;
		wordToCount = new HashMap<>();
		impactScore = im_score;
	}
};

class Q4FieldParamToColName
{
	public static final String[] T_ID = new String[] {"tweetid", "tid"};
	public static final String[] T_STAMP = new String[] {"timestamp", "Created_at"};
	public static final String[] HASH_TAG = new String[] {"hashtag", "Hashtags"};
	public static final String[] U_ID = new String[] {"userid", "Uid"};
	public static final String[] U_NAME = new String[] {"username", "U_name"};
	public static final String[] TEXT = new String[] {"text", "T_text"};
	
	public static String fieldNameToColName(String fieldName)
	{
		if (fieldName.equals(T_ID[0])) return T_ID[1];
		if (fieldName.equals(T_STAMP[0])) return T_STAMP[1];
		if (fieldName.equals(HASH_TAG[0])) return HASH_TAG[1];
		if (fieldName.equals(U_ID[0])) return U_ID[1];
		if (fieldName.equals(U_NAME[0])) return U_NAME[1];
		if (fieldName.equals(TEXT[0])) return TEXT[1];
		return "";
	}
}


public class MySQLTask {
    String teamID = TeamInfo.TEAM_ID;
    String AWS = TeamInfo.AWS_ACCOUNT_ID;

    final JDBCClient client;
    final RoutingContext event;
    
    public MySQLTask(JDBCClient jdbcClient, RoutingContext event) {
    	this.client = jdbcClient;
    	this.event = event;
    }
    
    public void ExecuteQuery4(Long tid, String op, Integer seq, String field, String payload) {
        try {
        	System.currentTimeMillis();
        	client.getConnection(res->{
				if (res.succeeded()) {
					System.currentTimeMillis();
					final SQLConnection connection = res.result();
					
					String queryString = "";
					
					final String colName = Q4FieldParamToColName.fieldNameToColName(field);
					if (colName.equals(""))
					{
						System.out.println("wrong field");
						event.response().end("No field: " + field);
						return;
					}
					
					if (op.equals("read"))
					{
						queryString = "select " + colName + " from shard where tid = ? limit 1";
						connection.queryWithParams(queryString, new JsonArray().add(tid), qres->{
							if (qres.succeeded())
							{
								System.currentTimeMillis();
								ResultSet resultSet = qres.result();
								List<JsonObject> rows = resultSet.getRows();
								connection.close();
								StringBuffer responseString = new StringBuffer();
								
								if (rows.size() == 0)
								{
									responseString.append(TeamInfo.TEAM_ID).append(',').append(TeamInfo.AWS_ACCOUNT_ID).append('\n');
								}
								else
								{
									Object val = rows.get(0).getValue(colName);
									if (val != null && val.toString().length() != 0)
									{
										responseString.append(TeamInfo.TEAM_ID).append(',').append(TeamInfo.AWS_ACCOUNT_ID).append('\n')
											.append(val).append('\n');
									}
									else
									{
										responseString.append(TeamInfo.TEAM_ID).append(',').append(TeamInfo.AWS_ACCOUNT_ID).append('\n');
									}
								}

								System.currentTimeMillis();
								event.response().putHeader("charset", "utf-8").end(responseString.toString());
								System.currentTimeMillis();
								//System.out.println(String.format("Q4: Conn: %d Query %d Load %d Algo %d", l_conn - l0, l_q - l_conn, l_r - l_q, l_alg - l_r));
								System.out.println("4");
							}
							else
							{
								qres.cause().printStackTrace();
								event.response().end("fail to query");
							}
						});
					}
					else if (op.equals("write"))
					{
						queryString = "update shard set " + colName + " = ? where tid = ?";
						connection.updateWithParams(queryString, new JsonArray().add(payload).add(tid), qres->{
							if (qres.succeeded())
							{
								System.currentTimeMillis();
								connection.close();
								System.currentTimeMillis();
								System.currentTimeMillis();
								
					        	StringBuffer sBuilder = new StringBuffer();
					        	sBuilder.append(TeamInfo.TEAM_ID).append(",").append(TeamInfo.AWS_ACCOUNT_ID)
					        		.append("\n").append("success").append("\n");
					        	System.out.println("4");
					        	event.response().putHeader("charset", "utf-8").end(sBuilder.toString());
							}
							else
							{
								qres.cause().printStackTrace();
								event.response().end("Query fails");
							}
						});
					}
					else if (op.equals("delete"))
					{
						queryString = "update shard set " + colName + " = NULL where tid = ?";
						connection.updateWithParams(queryString, new JsonArray().add(tid), qres->{
							if (qres.succeeded())
							{
								//long l_q = System.currentTimeMillis();
								connection.close();
								//long l_r = System.currentTimeMillis();
								//long l_alg = System.currentTimeMillis();
								//System.out.println(String.format("Q4: Conn: %d Query %d Load %d Algo %d", l_conn - l0, l_q - l_conn, l_r - l_q, l_alg - l_r));
								
								StringBuffer sBuilder = new StringBuffer();
								sBuilder.append(TeamInfo.TEAM_ID).append(",").append(TeamInfo.AWS_ACCOUNT_ID)
				        			.append("\n").append("success").append("\n");
								System.out.println("4");
								event.response().putHeader("charset", "utf-8").end(sBuilder.toString());
							}
							else
							{
								qres.cause().printStackTrace();
								event.response().end("Query fails");
							}
						});
					}
					else
					{
						event.response().end("Query fails");
						return;
					}
				}
				else {
					event.response().end("fail to connect to DB");
				}
			});
        } catch (Exception e) {
            e.printStackTrace();
            event.response().end("Exception occured");
        }
    }
    
    public void ExecuteQuery2(Long user1, Long user2, Integer n) {
        try {
        	//long l0 = System.currentTimeMillis();
        	client.getConnection(res->{
				if (res.succeeded()) {
					//long l_conn = System.currentTimeMillis();
					final SQLConnection connection = res.result();
					connection.queryWithParams("select * from twitter where uid = ? or uid = ? limit 2;", new JsonArray().add(Long.valueOf(user1)).add(Long.valueOf(user2)),
							qres->{
						if (qres.succeeded()) {
						//	long l_q = System.currentTimeMillis();
							ResultSet resultSet = qres.result();
							List<JsonObject> rows = resultSet.getRows();
							connection.close();
							
							final List<FullTweet> user1Tweet = new ArrayList<>();
							final List<FullTweet> user2Tweet = new ArrayList<>();
							for (JsonObject row : rows) {
								String[] tweetInfoPieces = row.getString("tweets").trim().split("\t"); // tid time word_counts
								String[] impactScores = row.getString("Scores").trim().split("\t");
								String[] censorTexts = row.getString("Censortext").split("\t");
								
								Long uid = row.getLong("uid");
								
								if ((tweetInfoPieces.length / 3) != impactScores.length || impactScores.length != censorTexts.length) {
									System.out.println("Data not correct!");
									event.response().end("Wrong data");
									return;
								}
								
								List<FullTweet> list = uid.equals(user1) ? user1Tweet : user2Tweet;
								
								for (int i = 0; i < impactScores.length; ++i)
								{
									FullTweet t = new FullTweet(uid, Long.valueOf(tweetInfoPieces[i*3]), tweetInfoPieces[i*3+1], censorTexts[i], Integer.valueOf(impactScores[i]));
									String[] wordCount = tweetInfoPieces[i*3+2].trim().split("\\s+");
									for (int j = 0; j < wordCount.length; j += 2)
									{
										if (j >= wordCount.length - 1) continue;
										t.wordToCount.put(wordCount[j], Integer.valueOf(wordCount[j + 1]));
									}
									list.add(t);
								}
							}
							
							//long l_r = System.currentTimeMillis();
							event.response().putHeader("charset", "utf-8").end(TeamInfo.TEAM_ID + ',' + TeamInfo.AWS_ACCOUNT_ID + '\n' + Query2Solution.GetSolution(user1Tweet, user2Tweet, n).toString());
							//long l_alg = System.currentTimeMillis();
							//System.out.println(String.format("Q2: Conn: %d Query %d Load %d Algo %d", l_conn - l0, l_q - l_conn, l_r - l_q, l_alg - l_r));
							System.out.println("2");
						}
						else {
							qres.cause().printStackTrace();
							event.response().end("fail to query");
						}
					});
				}
				else {
					event.response().end("fail to connect to DB");
				}
			});
        } catch (Exception e) {
            e.printStackTrace();
            event.response().end("Exception occured");
        }
    }
    
    /**
     * 
     * @param utc_start : concatenation of uid_start(20 chars), tid_start(20 chars) and created_at_start(10 chars)
     * @param utc_end : concatenation of uid_end(20 chars), tid_end(20 chars) and created_at_end(10 chars)
     * @param prefixes : prefixes
     */
	public void ExecuteQuery3(long uid_start, long uid_end, long tid_start, long tid_end, String date_start, String date_end, String[] prefixes) {
	    try {
	    	//long l_conn = System.currentTimeMillis();
	    	client.getConnection(res->{
				if (res.succeeded()) {
					final SQLConnection connection = res.result();
					//long l_query = System.currentTimeMillis();
					connection.queryWithParams("select tweets from twitter_q3 where uid between ? and ?",
							new JsonArray().add(uid_start).add(uid_end), 
							qres->{
								
						if (qres.succeeded()) {
							//long l_algo = System.currentTimeMillis();
							ResultSet resultSet = qres.result();
							List<JsonObject> rows = resultSet.getRows();
							connection.close();
							
							// --- init
							int[] num_prefixes = new int[prefixes.length];
							for (int i = 0; i < num_prefixes.length; ++i) {
								num_prefixes[i] = 0;
							}
							// ---
							// --- read rows in result
							for (JsonObject row : rows) {
								String[] tweetPieces = row.getString("tweets").split("\t");
								if (tweetPieces.length % 3 != 0) {
									event.response().end("Wrong data!");
									return;
								}
								// tweetPieces: tid \t created_at \t [word count]*
								for (int i = 0; i < tweetPieces.length; i += 3) {
									long tid = Long.valueOf(tweetPieces[i]);
									if ((tid > tid_end) || (tid < tid_start)) {
										continue;
									}
									
									String created_at = tweetPieces[i + 1].substring(0, 10);
									if (created_at.compareTo(date_start) < 0 || created_at.compareTo(date_end) > 0) {
										continue;
									}
									
									String[] words = tweetPieces[i + 2].trim().split("\\s+");
									
									for (int j = 0; j < words.length; j += 2)
									{
										if (j >= words.length - 1) continue; // For
										
										if (FrontEnd.skipWordsForQ3.contains(words[j]))
											continue;
										for (int k = 0; k < prefixes.length; ++k)
										{
											if (words[j].startsWith(prefixes[k]))
											{
												num_prefixes[k] += Integer.valueOf(words[j + 1]);
											}
										}
									}
								}
							}
							try {
								StringBuilder result = new StringBuilder();
								result.append(TeamInfo.TEAM_ID).append(TeamInfo.AWS_ACCOUNT_ID).append("\n");
								for (int i = 0; i < prefixes.length; ++i) {
									result.append(prefixes[i])
										.append(":")
										.append(num_prefixes[i])
										.append("\n");
								}
								event.response().putHeader("charset", "utf-8").end(result.toString());
								//long l_fin = System.currentTimeMillis();
								//System.out.println(String.format("Q3: CONN %s QUERY %s ALGO %s", l_query - l_conn, l_algo - l_query, l_fin - l_algo));
								System.out.println("3");
							} catch (Exception e) {
								e.printStackTrace();
							}
							// ---
						}
						else {
							qres.cause().printStackTrace();
							event.response().end("fail to query");
						}
					});
				}
				else {
					event.response().end("fail to connect to DB");
				}
			});
	    } catch (Exception e) {
	        e.printStackTrace();
	        event.response().end("Exception occured");
	    }
	}
}