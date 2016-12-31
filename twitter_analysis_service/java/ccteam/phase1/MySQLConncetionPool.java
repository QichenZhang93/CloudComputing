package ccteam.phase1;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

public class MySQLConncetionPool {
	private static final BasicDataSource BASIC_DATA_SOURCE = new BasicDataSource();
	
	static {
		BASIC_DATA_SOURCE.setDriverClassName("com.mysql.jdbc.Driver");
		BASIC_DATA_SOURCE.setUrl("jdbc:mysql://localhost:3306/ccteam?useSSL=false");
		BASIC_DATA_SOURCE.setUsername(System.getenv("sqlun"));
		BASIC_DATA_SOURCE.setPassword(System.getenv("sqlpw"));
	}
	
	public static Connection getConnection() {
		try {
			return BASIC_DATA_SOURCE.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
