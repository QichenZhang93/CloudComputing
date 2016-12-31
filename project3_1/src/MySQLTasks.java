import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLTasks {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "song_db";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static String DB_USER = "root"; // root -- qichenz
    private static String DB_PWD = "root"; // root -- db15319root
    
    private static Connection conn;

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in one single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) {
        try {
            DB_USER = System.getenv("id");
            DB_PWD = System.getenv("pw");
            initializeConnection();
            // This argument should be used to determine the piece(s) of your code to run.
            String runOption = args[0];
            switch (runOption) {
            // Run the demo function.
            case "demo":
                demo();
                break;
            // Load data from the csv files into corresponding tables.
            case "load_data":
                loadData();
                break;
            // Answer question 7.
            case "q7":
                q7();
                break;
            // Answer question 8.
            case "q8":
                // For q8, there should be an args[1] which is the name (NOT field) of your intended database index.
                q8(args[1]);
                break;
            // Answer question 9.
            case "q9":
                q9();
                break;
            // Answer question 10.
            case "q10":
                q10();
                break;
            // Answer question 11.
            case "q11":
                q11();
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Initializes database connection.
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }

    /**
     * JDBC usage demo. The following function will print the row count of the "songs" table.
     * Table must exists before this function is called.
     */
    private static void demo() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String tableName = "songs";
            String sql = "SELECT count(*) AS cnt FROM " + tableName;
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                int rowCount = rs.getInt("cnt");
                System.out.println("Total number of lines in " + tableName + " is: " + rowCount);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Load data.
     * 
     * This method should load data from csv files into corresponding tables.
     * Complete this method with your own implementation.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void loadData() {
    }

    /**
     * Question 7.
     * 
     * This method should execute a SQL query and print the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q7() {
        PreparedStatement pStatement = null;
        try {
            pStatement = conn.prepareStatement("select track_id from songs order by duration DESC limit 1;");
            ResultSet resultSet = pStatement.executeQuery();
            resultSet.next();
            System.out.println(resultSet.getString(1));
            resultSet.close();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Question 8.
     * 
     * A database index is a data structure that improves the speed of data retrieval.
     * Identify the field that will improve the performance of your query in question 7
     * and create a database index on that field. A custom index name is needed to create an index.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     *
     * @param indexName The name of your index (this is NOT the field on which your index will be created).
     */
    private static void q8(String indexName) {
        Statement pStatement = null;
        try {
            pStatement = conn.createStatement();
            String query = "create index "+ indexName +" on songs(duration) using BTREE;";
            pStatement.execute(query);
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Question 9.
     * 
     * This method should execute a SQL query and return the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * This is the same query as Question 7. Do you see any difference in performance?
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * 
     * Hint: Please create a private function and reuse it in q7 and q9.
     */
    private static void q9() {
        PreparedStatement pStatement = null;
        try {
            pStatement = conn.prepareStatement("select track_id from songs order by duration DESC limit 1;");
            ResultSet resultSet = pStatement.executeQuery();
            resultSet.next();
            System.out.println(resultSet.getString(1));
            resultSet.close();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Question 10.
     * 
     * Write the SQL query that returns all matches (across any column), similar to the command grep -P 'The Beatles' | wc -l:
     * Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q10() {
        PreparedStatement pStatement = null;
        try {
            pStatement = conn.prepareStatement("select count(*) from songs where title like binary ? or song_id like binary ? or `release` like binary ? or artist_id like binary ? or artist_mbid like binary ? or artist_name like binary ?");
            for (int i = 1; i <= 6; ++i) {
                pStatement.setString(i, "%The Beatles%");
            }
            ResultSet resultSet = pStatement.executeQuery();
            resultSet.next();
            System.out.println(resultSet.getInt(1));
            resultSet.close();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Question 11.
     * 
     * Which artist has the third-most number of rows in table songs? The output should be the name of the artist.
     * Please use artist_id as the unique identifier of the artist.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q11() {
        PreparedStatement pStatement = null;
        try {
            pStatement = conn.prepareStatement("select artist_name from (select count(*) as cnt, artist_id as id from songs group by artist_id order by cnt DESC limit 1 offset 2) as cnt_id left join songs on cnt_id.id = songs.artist_id limit 1;");
            ResultSet resultSet = pStatement.executeQuery();
            resultSet.next();
            System.out.println(resultSet.getString(1));
            resultSet.close();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
