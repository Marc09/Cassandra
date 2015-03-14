/*
Copyright 2015 Red Hat, Inc. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package uk.ac.ncl.cs.csc8101.weblogcoursework;


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * Integration point for application specific processing logic.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 */
public class MessageHandler {
    public static long MAX_IDLE_MS = TimeUnit.MINUTES.toMillis(30);
    private final static Cluster cluster;
    private final static Session session;
    private String info[];
    private String date[]; 
    private String url;
    final static PreparedStatement insertPS_1;
    static ResultSetFuture mutationFuture;
    final static PreparedStatement insertPS_2;
    Date time;
    Long timeLong;
    DateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    //check if the session should be expired, which is idle time bigger than 30min and got no new message about this session
    static HashMap<String,SiteSession> sessions = new LinkedHashMap<String,SiteSession>() {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            SiteSession siteSession = (SiteSession)eldest.getValue();
            boolean shouldExpire = siteSession.isExpired();
            if(shouldExpire) {
            	addToDatabase(siteSession);
            }
            return siteSession.isExpired();
        }
    };
    
    static {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();
        
        
        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS csc8101 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.close();

        session = cluster.connect("csc8101");
        session.execute("DROP TABLE IF EXISTS part1_table");
        session.execute("DROP TABLE IF EXISTS part2_table");
        session.execute("CREATE TABLE IF NOT EXISTS part1_table(url text, time bigint, counters counter, PRIMARY KEY (url, time) )");
        session.execute("CREATE TABLE IF NOT EXISTS part2_table(client_id text, start_time timestamp, end_time timestamp, "
        		+ "number_of_accesses bigint, number_of_distinct_urls bigint,  PRIMARY KEY (client_id, start_time) )");
        insertPS_1 = session.prepare("UPDATE part1_table SET counters=counters+? WHERE url=? and time=?");
        insertPS_2 = session.prepare("INSERT INTO part2_table (client_id, start_time, end_time, number_of_accesses, number_of_distinct_urls) VALUES (?, ?, ?, ?, ?)");
    }

    public static void close() {
        session.close();
        cluster.close();
    }

    public void flush() {
    	
    }
    
    public static void addToDatabase(SiteSession siteSession){
    	 mutationFuture = session.executeAsync(new BoundStatement(insertPS_2).bind(
    			 																siteSession.getId(), 
    			 																new Timestamp(siteSession.getFirstHitMillis()), 
    			 																new Timestamp(siteSession.getLastHitMillis()), 
    			 																siteSession.getHitCount(), 
    			 																siteSession.getHyperLogLog().cardinality()));
    }

    public void handle(String message) {
    	/*  
    	 * client_id timestamp "method url version" status size
    	 * 0  	1                     2      3    4                    5         6   7
    	 * 4453 [01/May/1998:02:58:38 +0000] "GET /images/hm_day_e.gif HTTP/1.1" 200 499
    	 * 3834 [01/May/1998:02:58:38 +0000] "GET /images/32p49802.jpg HTTP/1.1" 200 11754
    	 * 4453 [01/May/1998:02:58:38 +0000] "GET /images/hm_brdl.gif HTTP/1.1" 200 20
    	 * 4470 [01/May/1998:02:58:38 +0000] "GET /images/dot.gif HTTP/1.0" 200 43
    	 */
     	try{
     		//split the log using space
     		info = message.split(" ");
     		url = info[4];
     		date = info[1].split(":");
            mutationFuture = session.executeAsync( new BoundStatement(insertPS_1).bind(1L, url, Long.valueOf(date[1])) );
            
            time = sdf.parse(info[1].substring(1));
            timeLong = time.getTime();
            /*check if the hashmap contains the client_id, if not, put the id and session in the hashmap
             * if contains, compare the log time with the sessions first hit time
             * 				if time difference is smaller than 30min, update session in the hashmap
             * 				if time difference is bigger than 30min, put the session into the table 
            */
            if(!sessions.containsKey(info[0])){
            	sessions.put(info[0], new SiteSession(info[0], timeLong, info[4]));
            } else {
            	if (timeLong - sessions.get(info[0]).getFirstHitMillis() < MAX_IDLE_MS ){
                	sessions.get(info[0]).update(timeLong, info[4]);
            	} else {
                    mutationFuture = session.executeAsync(new BoundStatement(insertPS_2).bind(
                    										info[0], 
                    										new Timestamp(sessions.get(info[0]).getFirstHitMillis()), 
                    										new Timestamp(sessions.get(info[0]).getLastHitMillis()), 
                    										sessions.get(info[0]).getHitCount(), 
                    										sessions.get(info[0]).getHyperLogLog().cardinality()));
                	sessions.replace(info[0], new SiteSession(info[0], timeLong, info[4]));
            	}
            }
     	} catch (Exception e){
     		e.printStackTrace();
     	}
    }
}
