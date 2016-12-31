package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.json.JSONObject;

public class ProfileServlet extends HttpServlet {
	
    private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String DB_NAME = "cc33";
    private final String URL = "jdbc:mysql://mysqlzqc.clsgyurpyjid.us-east-1.rds.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private String DB_USER = System.getenv("mysqlun"); // root -- qichenz
    private String DB_PWD = System.getenv("mysqlpd"); // root -- db15319root

    private Connection conn;
    
    private void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }
    
    public ProfileServlet() {
        /*
            Your initialization code goes here
        */
    	try {
			initializeConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        final String id = request.getParameter("id");
        final String pwd = request.getParameter("pwd");

        /*
            Task 1:
            This query simulates the login process of a user, 
            and tests whether your backend system is functioning properly. 
            Your web application will receive a pair of UserID and Password, 
            and you need to check in your backend database to see if the 
	    UserID and Password is a valid pair. 
            You should construct your response accordingly:

            If YES, send back the user's Name and Profile Image URL.
            If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
        */

        String username = "Unauthorized";
        String imgUrl = "#";
        //System.out.println(id + ' ' + pwd);
        PreparedStatement statement = null;
        try {
        	statement = conn.prepareStatement("select userinfo.uname, userinfo.profile_img_url from userinfo inner join (select id from users where id = ? and pw = ?) as u on userinfo.id = u.id;");
        	statement.setString(1, id);
        	statement.setString(2, pwd);
        	ResultSet rset = statement.executeQuery();
        	if (rset.next()) {
        		username = rset.getString(1);
        		imgUrl = rset.getString(2);
        		//System.out.println(username + '\n' + imgUrl);
        	}
        	statement.close();
        }
        catch (SQLException e) {
        	e.printStackTrace();
		}
        result.put("name", username);
        result.put("profile", imgUrl);
        
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
