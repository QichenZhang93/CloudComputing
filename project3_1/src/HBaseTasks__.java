import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

public class HBaseTasks__ {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.57.58";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "songdata";
    /**
     * HTable handler.
     */
    private static HTableInterface songsTable;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Initialize HBase connection.
     * @throws IOException
     */
    private static void initializeConnection() throws IOException {
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
        conn = HConnectionManager.createConnection(conf);
        songsTable = conn.getTable(Bytes.toBytes(tableName));
        
    }

    /**
     * Clean up resources.
     * @throws IOException
     */
    private static void cleanup() throws IOException {
        if (songsTable != null) {
            songsTable.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in ONE single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) throws IOException {
        initializeConnection();
        switch (args[0]) {
        case "demo":
            demo();
            break;
        case "q12":
            q12();
            break;
        case "q13":
            q13();
            break;
        case "q14":
            q14();
            break;
        case "q15":
            q15();
            break;
        case "q16":
            q16();
        }
        cleanup();
    }

    /**
     * This is a demo of how to use HBase Java API. It will print all the artist_names starting with "The Beatles".
     * @throws IOException
     */
    private static void demo() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^The Beatles.*");
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }

    /**
     * Question 12.
     *
     * What was that song whose name started with "Total" and ended with "Water"?
     * Write an HBase query that finds the track that the person is looking for.
     * The title starts with "Total" and ends with "Water", both are case sensitive.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * @throws IOException 
     */
    private static void q12() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("title");
        RegexStringComparator comparator = new RegexStringComparator("^Total.*Water$");
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol, CompareOp.EQUAL, comparator);
        scan.setFilter(filter);
        scan.setBatch(1);
        ResultScanner rScanner = songsTable.getScanner(scan);
        Result result =rScanner.next();
        if (result != null) {
            System.out.println(result.getValue(bColFamily, bCol));
        }
        else {
            System.out.println("");
        }
    }

    /**
     * Question 13.
     *
     * I don't remember the exact title, it was that song by "Kanye West", and the
     * title started with either "Apologies" or "Confessions". Not sure which...
     * Write an HBase query that finds the track that the person is looking for.
     * The artist_name contains "Kanye West", and the title starts with either
     * "Apologies" or "Confessions" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * @throws IOException 
     */
    private static void q13() throws IOException {
        Scan scan = new Scan();
        byte[] bCol_artist_name = Bytes.toBytes("artist_name");
        byte[] bCol_title = Bytes.toBytes("title");
        RegexStringComparator comparator_name = new RegexStringComparator(".*Kanye West.*");
        RegexStringComparator comparator_title = new RegexStringComparator("^(Apologies|Confessions).*");
        FilterList filterList = new FilterList();
        
        Filter filter_name = new SingleColumnValueFilter(bColFamily, bCol_artist_name, CompareOp.EQUAL, comparator_name);
        Filter filter_title = new SingleColumnValueFilter(bColFamily, bCol_artist_name, CompareOp.EQUAL, comparator_title);
        filterList.addFilter(filter_title);
        filterList.addFilter(filter_name);
        scan.setFilter(filterList);
        ResultScanner rScanner = songsTable.getScanner(scan);
        Result result = null;
        boolean isFirst = true;
        for (result = rScanner.next(); result != null; result = rScanner.next()) {
            if (!isFirst) {
                System.out.print(" ");
            }
            else isFirst = false;
            System.out.print(result.getValue(bColFamily, bCol_title));
        }
        System.out.println("");
    }

    /**
     * Question 14.
     *
     * There was that new track by "Bob Marley" that was really long. Do you know?
     * Write an HBase query that finds the track the person is looking for.
     * The artist_name has a prefix of "Bob Marley", duration no less than 400,
     * and year 2000 and onwards (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * @throws IOException 
     */
    private static void q14() throws IOException {
        Scan scan = new Scan();
        byte[] bCol_artist_name = Bytes.toBytes("artist_name");
        byte[] bCol_year = Bytes.toBytes("year");
        byte[] bCol_duration = Bytes.toBytes("duration");
        byte[] bCol_title = Bytes.toBytes("title");
        
        RegexStringComparator comparator_name = new RegexStringComparator("^Bob Marley.*");
        BinaryComparator comparator_year = new BinaryComparator(Bytes.toBytes(2000)); // TODO
        BinaryComparator comparator_duration = new BinaryComparator(Bytes.toBytes(400)); // TODO
        
        FilterList filterList = new FilterList();
        Filter filter_name = new SingleColumnValueFilter(bColFamily, bCol_artist_name, CompareOp.EQUAL, comparator_name);
        Filter filter_year = new SingleColumnValueFilter(bColFamily, bCol_year, CompareOp.GREATER_OR_EQUAL, comparator_year);
        Filter filter_duration = new SingleColumnValueFilter(bColFamily, bCol_duration, CompareOp.GREATER_OR_EQUAL, comparator_duration);
        filterList.addFilter(filter_name);
        filterList.addFilter(filter_year);
        filterList.addFilter(filter_duration);
        
        scan.setFilter(filterList);
        
        ResultScanner rScanner = songsTable.getScanner(scan);
        Result result = null;
        boolean isFirst = true;
        for (result = rScanner.next(); result != null; result = rScanner.next()) {
            if (!isFirst) {
                System.out.print(" ");
            }
            else isFirst = false;
            System.out.print(result.getValue(bColFamily, bCol_title));
        }
        System.out.println("");
    }

    /**
     * Question 15.
     *
     * I heard a really great song about "Family" by this really cute singer,
     * I think his name was "Consequence" or something...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_hotttnesss of at least 1, and the artist_name
     * contains "Consequence". Also, the title contains "Family" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * @throws IOException 
     */
    private static void q15() throws IOException {
        Scan scan = new Scan();
        byte[] bCol_artist_name = Bytes.toBytes("artist_name");
        byte[] bCol_hot = Bytes.toBytes("artist_hotttnesss");
        byte[] bCol_title = Bytes.toBytes("title");
        
        RegexStringComparator comparator_artist_name = new RegexStringComparator(".*Consequence.*");
        BinaryComparator comparator_hot = new BinaryComparator(Bytes.toBytes(1));
        RegexStringComparator comparator_title = new RegexStringComparator(".*Family.*");
        
        FilterList filterList = new FilterList();
        Filter filter_artist_name = new SingleColumnValueFilter(bColFamily, bCol_artist_name, CompareOp.EQUAL, comparator_artist_name);
        Filter filter_hot = new SingleColumnValueFilter(bColFamily, bCol_hot, CompareOp.GREATER_OR_EQUAL, comparator_hot);
        Filter filter_title = new SingleColumnValueFilter(bColFamily, bCol_title, CompareOp.EQUAL, comparator_title);
        filterList.addFilter(filter_artist_name);
        filterList.addFilter(filter_hot);
        filterList.addFilter(filter_title);
        
        scan.setFilter(filterList);
        
        ResultScanner rScanner = songsTable.getScanner(scan);
        Result result = null;
        boolean isFirst = true;
        for (result = rScanner.next(); result != null; result = rScanner.next()) {
            if (!isFirst) {
                System.out.print(" ");
            }
            else isFirst = false;
            System.out.print(result.getValue(bColFamily, bCol_title));
        }
        System.out.println("");
    }

    /**
     * Question 16.
     *
     * Hey what was that "Love" song that "Gwen Guthrie" came out with in 1990?
     * No, no, it wasn't the sad one, nothing "Bitter" or "Never"...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_name prefix of "Gwen Guthrie", the title contains "Love"
     * but does NOT contain "Bitter" or "Never", the year equals to 1990.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q16() {
    }

}
