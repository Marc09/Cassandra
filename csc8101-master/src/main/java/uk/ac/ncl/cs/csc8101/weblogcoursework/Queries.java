package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Queries {
	private final static Cluster cluster;
    private final static Session session;
    Long sum = 0L;
    static {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS csc8101 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.close();

        session = cluster.connect("csc8101");
        for (int i = 0; i < 24; i++){
            session.execute("CREATE TABLE IF NOT EXISTS test_table" + i + "(url text, counter bigint, PRIMARY KEY (url) )");
        }
    }
    public static void main(String[] args){
    	String testID = String.valueOf(11895);
    	Queries test = new Queries();
    	test.part1Query();
    	test.part2Query(testID);
    }
    
    public static void close() {
        session.close();
        cluster.close();
    }
    
    public void part1Query(){
    	//For a given set of URLs, start hour and end hour, show the total number of accesses for each url doing that period
		final PreparedStatement selectPS = session.prepare("SELECT * FROM part1_table");
        ResultSet resultSet = session.execute( new BoundStatement(selectPS));
		for (Row row: resultSet) {
			System.out.println("url: " + row.getString("url") + " counters: " + row.getLong("counters"));
		}
    }
    
    public void part2Query(String client_id){
    	//For a given client id, show all sessions with their start time, end time, number of accesses and
		//approximate number of distinct URLs accessed.
		final PreparedStatement selectPS1 = session.prepare("SELECT * FROM part2_table where client_id=?");
        ResultSet resultSet1 = session.execute(new BoundStatement(selectPS1).bind(client_id));
		for (Row row: resultSet1) {
			System.out.println("client_id: "+row.getString("client_id") + "  start_time: "+row.getDate("start_time") +"  end_time: "+ row.getDate("end_time") 
					+ "  number_of_accesses: "+ row.getLong("number_of_accesses") + "  number_of_distinct_urls: " + row.getLong("number_of_distinct_urls"));
			sum = sum + row.getLong("number_of_distinct_urls");
		}    		
		
		//Provide an approximate number of distinct URLs over all the sessions for the given client id.
		 
		System.out.println("client_id: " + client_id +"  all sessions number_of_distinct_urls:" + sum);
    }
    }