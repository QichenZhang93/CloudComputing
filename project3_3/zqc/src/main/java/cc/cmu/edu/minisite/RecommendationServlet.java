package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;



public class RecommendationServlet extends HttpServlet {
	
	public RecommendationServlet () throws Exception {
        /*
        	Your initialization code goes here
         */
		
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {

		JSONObject result = new JSONObject();
	        String id = request.getParameter("id");

		/**
		 * Bonus task:
		 * 
		 * Recommend at most 10 people to the given user with simple collaborative filtering.
		 * 
		 * Store your results in the result object in the following JSON format:
		 * recommendation: [
		 * 		{name:<name_1>, profile:<profile_1>}
		 * 		{name:<name_2>, profile:<profile_2>}
		 * 		{name:<name_3>, profile:<profile_3>}
		 * 		...
		 * 		{name:<name_10>, profile:<profile_10>}
		 * ]
		 * 
		 * Notice: make sure the input has no duplicate!
		 */
        

        
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

