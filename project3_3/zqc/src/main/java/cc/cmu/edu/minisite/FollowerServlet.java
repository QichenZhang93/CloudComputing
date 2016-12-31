package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.json.JSONArray;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class Follower {
	public String Name;
	public String ImgURL;
	public Follower(String name, String imgUrl) {
		Name = name;
		ImgURL = imgUrl;
	}
}

public class FollowerServlet extends HttpServlet {
	
	/**
     * The private IP address of HBase master node.
     */
    private String zkAddr = "172.31.55.249"; // TODO:
    /**
     * The name of your HBase table.
     */
    private TableName tableName = TableName.valueOf("relations");
    /**
     * HTable handler.
     */
    private Table relationsTable;
    /**
     * HBase connection.
     */
    private Connection conn;
    
    /**
     * Logger.
     */
    private final Logger LOGGER = Logger.getRootLogger();
    
    private void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        LOGGER.setLevel(Level.ERROR);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = ConnectionFactory.createConnection(conf);
        relationsTable = conn.getTable(tableName);
    }


    public FollowerServlet() throws IOException {
        /*
            Your initialization code goes here
        */
    	initializeConnection();
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        System.out.println(id);

        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */
        
        Scan scan = new Scan();
        byte[] bCol_id = Bytes.toBytes("id");
        byte[] bCol_name = Bytes.toBytes("name");
        byte[] bCol_url = Bytes.toBytes("url");
        byte[] bColFamilyFollowee = Bytes.toBytes("followee");
        byte[] bColFamilyFollower = Bytes.toBytes("follower");
        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes(id));
        
        Filter filter = new SingleColumnValueFilter(bColFamilyFollowee, bCol_id, CompareOp.EQUAL, binaryComparator);
        
        scan.setFilter(filter);
        ResultScanner rScanner = relationsTable.getScanner(scan);
        Result rset = null;
        ArrayList<Follower> arrayList = new ArrayList<Follower>();
        for (rset = rScanner.next(); rset != null; rset = rScanner.next()) {
        	System.out.println("ADD");
            arrayList.add(new Follower(Bytes.toString(rset.getValue(bColFamilyFollower, bCol_name)), Bytes.toString(rset.getValue(bColFamilyFollower, bCol_url))));
        }
        System.out.println(arrayList.size());
        arrayList.sort(new Comparator<Follower>() {

			@Override
			public int compare(Follower o1, Follower o2) {
				if (o1.Name.equals(o2.Name)) {
					return o1.ImgURL.compareTo(o2.ImgURL);
				}
				return o1.Name.compareTo(o2.Name);
			}
		});
        JSONArray tmpArray = new JSONArray();
        JSONObject tmpObj = null;
        for (Follower follower : arrayList) {
        	System.out.println(follower.Name + " " + follower.ImgURL);
        	tmpObj = new JSONObject();
        	tmpObj.put("name", follower.Name).put("profile", follower.ImgURL);
        	tmpArray.put(tmpObj);
		}
        result.put("followers", tmpArray);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}


