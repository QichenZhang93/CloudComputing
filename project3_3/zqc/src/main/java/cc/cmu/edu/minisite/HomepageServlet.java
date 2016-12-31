package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import org.json.JSONObject;
import org.json.JSONArray;

public class HomepageServlet extends HttpServlet {
	
	MongoClient mongoClient; // = new MongoClient("54.164.40.165", 27017);
    MongoDatabase mongoDatabase; // = mongoClient.getDatabase("db_cc");

    public HomepageServlet() {
        /*
            Your initialization code goes here
        */
    	//mongoClient = new MongoClient("54.164.40.165", 27017);
        //mongoDatabase = mongoClient.getDatabase("db_cc");
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();

        /*
            Task 3:
            Implement your logic to return all the posts authored by this user.
            Return this posts as-is, but be cautious with the order.

            You will need to sort the posts by Timestamp in ascending order
	     (from the oldest to the latest one). 
        */
        MongoClient mongoClient = new MongoClient("54.164.40.165", 27017);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("db_cc");
        final ArrayList<Document> rArrayList = new ArrayList<Document>();
        FindIterable<Document> iterable= mongoDatabase.getCollection("posts").find(new Document("uid", Integer.valueOf(id))).sort(new Document("timestamp", 1));
        final JSONArray jarray = new JSONArray();
        iterable.forEach(new Block<Document>() {

        	@Override
			public void apply(Document document) {
        		//System.out.println(document);
        		document.remove("_id");
        		jarray.put(document);
			}
		});
        result.put("posts", jarray);
        mongoClient.close();
        System.out.println(result);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}

