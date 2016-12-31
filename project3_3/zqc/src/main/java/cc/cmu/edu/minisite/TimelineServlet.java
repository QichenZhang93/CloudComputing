package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.BinaryOperator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class TimelineServlet extends HttpServlet {
	
	class StringPair {
		public String first;
		public String second;
		public StringPair(String s1, String s2) {
			first = s1;
			second = s2;
		}
	}
	
	class HbaseJob {
		/**
	     * The private IP address of HBase master node.
	     */
	    public String zkAddr = "172.31.55.249"; // TODO:
	    /**
	     * The name of your HBase table.
	     */
	    public TableName tableName = TableName.valueOf("relations");
	    /**
	     * HTable handler.
	     */
	    public Table relationsTable;
	    /**
	     * HBase connection.
	     */
	    public Connection conn;
	    
	    /**
	     * Logger.
	     */
	    public final Logger LOGGER = Logger.getRootLogger();
	}
	
	class MysqlJob {
		public final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
		public final String DB_NAME = "cc33";
		public final String URL = "jdbc:mysql://mysqlzqc.clsgyurpyjid.us-east-1.rds.amazonaws.com/" + DB_NAME + "?useSSL=false";
		public String DB_USER = System.getenv("mysqlun"); // root -- qichenz
	    public String DB_PWD = System.getenv("mysqlpd"); // root -- db15319root

	    public java.sql.Connection conn;
	}
	
	class MongoDBJob {
		public MongoClient mongoClient;
	    public MongoDatabase mongoDatabase;
	}
	
	HbaseJob hbaseJob = new HbaseJob();
	MysqlJob mysqlJob = new MysqlJob();
	MongoDBJob mongoDBJob = new MongoDBJob();
	
	public void ConnectToHbase() throws IOException {
		// set up hbase
    	hbaseJob.LOGGER.setLevel(Level.ERROR);
        if (!hbaseJob.zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", hbaseJob.zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", hbaseJob.zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        hbaseJob.conn = ConnectionFactory.createConnection(conf);
        hbaseJob.relationsTable = hbaseJob.conn.getTable(hbaseJob.tableName);
	}
	
	public void EndConnectionToHbase() throws IOException {
		hbaseJob.conn.close();
	}
	
	public void ConnectToMySql() throws ClassNotFoundException, SQLException {
		Class.forName(mysqlJob.JDBC_DRIVER);
        mysqlJob.conn = DriverManager.getConnection(mysqlJob.URL, mysqlJob.DB_USER, mysqlJob.DB_PWD);
	}
	
	public void EndConnectionToMysql() throws SQLException {
		mysqlJob.conn.close();
	}
	
	public void ConnectToMongoDB() {
		mongoDBJob.mongoClient = new MongoClient("54.164.40.165", 27017);
		mongoDBJob.mongoDatabase = mongoDBJob.mongoClient.getDatabase("db_cc");
	}
	
	public void EndConnectionToMongoDB() {
		mongoDBJob.mongoClient.close();
	}

    public TimelineServlet() throws Exception {
        /*
            Your initialization code goes here
        */
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        final JSONObject result = new JSONObject();
        String id = request.getParameter("id");

        /*
            Task 4 (1):
            Get the name and profile of the user as you did in Task 1
            Put them as fields in the result JSON object
        */
        
        try {
			ConnectToMySql();
			PreparedStatement statement = mysqlJob.conn.prepareStatement("select uname, profile_img_url from userinfo where id = ?");
			statement.setInt(1, Integer.valueOf(id));
			ResultSet rSet = statement.executeQuery();
			if (rSet.next()) {
				String uname = rSet.getString(1);
				String profile = rSet.getString(2);
				result.put("name", uname);
				result.put("profile", profile);
			}
			EndConnectionToMysql();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        System.out.println("Got user name & profile");

        /*
            Task 4 (2);
            Get the follower name and profiles as you did in Task 2
            Put them in the result JSON object as one array
        */
        
        try {
        	final byte[] bColFollowerColumnFamily = Bytes.toBytes("follower");
        	final byte[] bColFolloweeColumnFamily = Bytes.toBytes("followee");
        	final byte[] bColId = Bytes.toBytes("id");
        	final byte[] bColName = Bytes.toBytes("name");
        	final byte[] bColUrl = Bytes.toBytes("url");
        	
        	final BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes(id));
        	ConnectToHbase();
        	// Get followers name & profile
        	org.apache.hadoop.hbase.filter.Filter filter = 
        			new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
        					bColFolloweeColumnFamily, 
        					bColId, 
        					org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL, binaryComparator);
        	org.apache.hadoop.hbase.client.Scan scan = new Scan();
        	scan.setFilter(filter);
        	org.apache.hadoop.hbase.client.ResultScanner resultScanner = hbaseJob.relationsTable.getScanner(scan);
        	org.apache.hadoop.hbase.client.Result rSet = null;
        	JSONArray followersJSONArray = new JSONArray();
        	ArrayList<StringPair> followerNameProfilePair = new ArrayList<TimelineServlet.StringPair>();
        	for (rSet = resultScanner.next(); rSet != null; rSet = resultScanner.next()) {
        		String followerNameFromHbase = org.apache.hadoop.hbase.util.Bytes.toString(rSet.getValue(bColFollowerColumnFamily, bColName));
        		String followerUrlFromHbase = org.apache.hadoop.hbase.util.Bytes.toString(rSet.getValue(bColFollowerColumnFamily, bColUrl));
        		followerNameProfilePair.add(new StringPair(followerNameFromHbase, followerUrlFromHbase));
        	}
        	followerNameProfilePair.sort(new Comparator<StringPair>() {

        		@Override
				public int compare(StringPair o1, StringPair o2) {
        			if (o1.first.equals(o2.first)) {
        				return o1.second.compareTo(o2.second);
        			}
					return o1.first.compareTo(o2.first);
				}
			});
        	
        	for (StringPair stringPair : followerNameProfilePair) {
        		followersJSONArray.put(new JSONObject().put("name", stringPair.first).put("profile", stringPair.second));
			}
        	resultScanner.close();
        	EndConnectionToHbase();
        	result.put("followers", followersJSONArray);
        	System.out.println("Got followers : \n" + followersJSONArray);
        	
        	ConnectToHbase();
        	
        	// Get followees id
        	final ArrayList<String> followeeIdFromHBase = new ArrayList<String>();
        	filter = new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
        					bColFollowerColumnFamily, 
        					bColId, 
        					org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL, binaryComparator);
        	scan = new Scan();
        	scan.setFilter(filter);
        	resultScanner = hbaseJob.relationsTable.getScanner(scan);
        	rSet = null;
        	for (rSet = resultScanner.next(); rSet != null; rSet = resultScanner.next()) {
        		String followeeId = org.apache.hadoop.hbase.util.Bytes.toString(rSet.getValue(bColFolloweeColumnFamily, bColId));
        		System.out.println(followeeId);
        		followeeIdFromHBase.add(followeeId);
        	}
        	resultScanner.close();
        	EndConnectionToHbase();
        	System.out.println("Got followees id");
        	
            /*
            	Task 4 (3):
            	Get the 30 LATEST followee posts and put them in the
            	result JSON object as one array.
	
            	The posts should be sorted:
            	First in ascending timestamp order
            	Then numerically in ascending order by their PID (PostID) 
	    		if there is a tie on timestamp
             */
        	// Get posts
        	ConnectToMongoDB();
        	ArrayList<org.bson.Document> followeeIdDocumentList = new ArrayList<org.bson.Document>();
        	for (String followeeId : followeeIdFromHBase) {
        		followeeIdDocumentList.add(new Document("uid", Integer.valueOf(followeeId)));
			}
        	FindIterable<Document> iterable = 
        			mongoDBJob.mongoDatabase.getCollection("posts")
        			.find(new Document("$or", followeeIdDocumentList))
        			.sort(new Document("timestamp", 1).append("pid", 1));
        	final JSONArray postsJSONArray = new JSONArray();
        	final ArrayList<org.bson.Document> postsArrayFromMongoDB = new ArrayList<Document>();
        	iterable.forEach(new Block<org.bson.Document>() {
        		
        		@Override
				public void apply(Document doc) {
					//postsJSONArray.put(doc);
        			postsArrayFromMongoDB.add(doc);
				}
			});
        	int i = postsArrayFromMongoDB.size() - 30;
        	if (i < 0) i = 0;
        	for (; i < postsArrayFromMongoDB.size(); ++i) {
        		postsJSONArray.put(postsArrayFromMongoDB.get(i));
        	}
        	System.out.println(postsJSONArray);
        	result.put("posts", postsJSONArray);
        	EndConnectionToMongoDB();
        	System.out.println("Got posts");
        }
        catch (Exception e) {
        	e.printStackTrace();
		}

        
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
}

