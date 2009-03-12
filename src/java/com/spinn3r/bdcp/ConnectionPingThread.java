/*
 * Copyright 2009 Tailrank, Inc (Spinn3r).
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * For more information see:
 * 
 * <a href="http://tailrank.com">http://tailrank.com</a>
 * <a href="http://spinn3r.com">http://spinn3r.com</a>
 * <a href="http://feedblog.org">http://feedblog.org</a>
 */

package com.spinn3r.bdcp;

import java.util.*;
import java.sql.*;

import org.apache.log4j.*;

/**
 * A thread which does a SELECT 1 from each idle connection to prevent them from
 * timing out.
 */
public class ConnectionPingThread extends Thread {

    private static Logger log = Logger.getLogger( ConnectionPingThread.class );

    public static long INTERVAL = 60L * 60L * 1000L;

    public static String PING_COMMAND = "SELECT 1";

    public BasicDatabaseConnectionPool pool = null;

    public ConnectionPingThread( BasicDatabaseConnectionPool pool ) {

        super( "JDBC connection pinger" );

        this.pool = pool;
        this.setDaemon( true );

    }
    
    public void run() {

        while( true ) {

            try { 
                Thread.sleep( INTERVAL );
            } catch ( Exception e ) { }

            synchronized( pool.idleConnections ) {

                Iterator it = pool.idleConnections.iterator();

                while( it.hasNext() ) {

                    //run ping on the command...

                    try { 
                        
                        Connection conn = (Connection)it.next();
                        
                        Statement stmt = conn.createStatement();
                        ResultSet results = stmt.executeQuery( PING_COMMAND );
                        
                        results.close();
                        stmt.close();
                        
                    } catch ( Exception e ) {
                        log.error( "Unable to ping: ", e );
                    }

                }

            }

        }

    }
            
}