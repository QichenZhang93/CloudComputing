package ccteam.phase1;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;

public class VertxJDBC {
	public static JDBCClient CLIENT = null;
	
	static {
		JsonObject config = new JsonObject().put("url", "jdbc:mysql://localhost:3306/ccteam?useSSL=false")
				.put("driver_class", "org.hsqldb.jdbcDriver")
				.put("max_pool_size", 30);
		
	}
}