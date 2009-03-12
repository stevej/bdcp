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
import java.io.*;

import java.sql.*;
import javax.sql.*;

/**
 * Just like BasicDatabaseConnectionPool but we use a FIFO queue instead of a
 * spin mechanism to return objects to the caller.  This is a simple mechanism
 * which won't result in too much CPU load, doesn't require tuning, and
 * guarantees that callers get connections in the order they requested them.
 */
public class BasicDatabaseConnectionPool2 
    extends BasicDatabaseConnectionPool 
    implements DataSource {

    /**
     * The default maximum amount of time (in millis) the
     * {@link #borrowObject} method should block before throwing
     * an exception when the pool is exhausted and the
     * {@link #getWhenExhaustedAction "when exhausted" action} is
     * {@link #WHEN_EXHAUSTED_BLOCK}.
     * @see #getMaxWait
     * @see #setMaxWait
     */
    public static final long DEFAULT_MAX_WAIT = -1L;

    /**
     * The default cap on the total number of active instances from the pool
     * (per key).
     * @see #getMaxActive
     * @see #setMaxActive
     */
    public static final int DEFAULT_MAX_ACTIVE  = 10;

    public static final boolean DEFAULT_ENABLE_TRACKING  = false;

    /**
     * Wait for the pool to have a new object.  Its important for this be high
     * enough to prevent CPU thrash but short enough to check often.  In the
     * future we might want to tune this at runtime.
     */
    public static int WAIT_INTERVAL = 30000;

    /**
     * The list of idle connections available for pool use by calling threads.
     */
    List idleConnections = new LinkedList();

    /**
     * The list of current connections that are thought to be active.
     */
    HashMap trackedConnections = new HashMap();

    public int initialSize = 0;

    public int maxActive = DEFAULT_MAX_ACTIVE;
    public long maxWait = DEFAULT_MAX_WAIT;

    public int totalActive = 0;
    public int totalIdle = 0;

    /**
     * Enable connection tracking by default?
     */
    public boolean enableTracking = DEFAULT_ENABLE_TRACKING;
    
    // **** JDBC connection params **********************************************

    public String driver = null;
    public String user = null;
    public String password = null;
    public String url = null;

    /**
     * Linked list of objects needing notify() for resulting connections.
     */
    public LinkedList fifo = new LinkedList();

    public BasicDatabaseConnectionPool2() {

        ConnectionPingThread pingthread = new ConnectionPingThread( this );
        pingthread.start();

    }

    /**
     * Called AFTER we've set all necessary variables.
     */
    public void initialize() { }
    
    public Connection getConnection() throws SQLException {

        long duration = 0;
            
        while ( true ) {

            //we already have connections.

            synchronized( idleConnections ) {

                Connection conn;

                //FIXME: this remove is REALLY expensive and apparently we're
                //spending a LOT of time here according to my benchmarks with
                //JProfiler.  I'm not sure if this is a false because its a
                //getConnection() method so there might be blocked IO here.
                //

                // NOTE: the majore problem is that I'll need an object
                // structure that I can have high transaction synchronized
                // add/remove

                // NOTE: it looks like commons DBCP maintains a timestamp ->
                // object map as a CursorableLinkedList that it gets from
                // commons collections.

                if ( idleConnections.size() > 0 ) {

                    conn = (Connection)idleConnections.remove( 0 );
                    --totalIdle;

                    return addTrackedConnection( new BasicDatabaseConnection( conn, this ) );
                    
                }

                //create a new connection as we haven't hit our limit
                if ( idleConnections.size() == 0 && totalActive < maxActive ) {
                    return addTrackedConnection( createConnection() );
                }

            }

            //no more connections.  Wait for one.
            try { 

                //FIXME: I think spinning defeats teh WHOLE point rigght!?!?!!!!

                ConnectionMonitor monitor = new ConnectionMonitor();

                //FIXME: I think this is a problem that there might be a gap in
                //time where we could blead conns
                
                //add this to the queue for processing once a conn is available.
                synchronized( fifo ) {
                    fifo.addFirst( monitor );
                }

                synchronized( monitor ) {
                    monitor.wait( WAIT_INTERVAL );
                    
                    if ( monitor.conn != null )
                        //This will already be wrapped.
                        return monitor.conn;

                }
                
            } catch ( InterruptedException e ) { /* ignore interrupt() issues*/ }

        }

    }

    public BasicDatabaseConnection createConnection() throws SQLException {

        Connection conn = DriverManager.getConnection( url, user, password );
        ++totalActive;

        return new BasicDatabaseConnection( conn, this );
        
    }
        
    public void returnObject( BasicDatabaseConnection conn ) {

        //process pending requests in our FIFO queue.
        synchronized( fifo ) {

            //the performance of size() is O(1)
            if ( fifo.size() > 0 ) {

                ConnectionMonitor monitor = (ConnectionMonitor)fifo.removeLast();

                if ( monitor != null ) {
                    
                    BasicDatabaseConnection c = 
                        new BasicDatabaseConnection( conn.getDelegate(),
                                                     this );

                    monitor.conn = addTrackedConnection( c );
                
                    synchronized( monitor ) {
                        monitor.notify();
                    }
                    
                    return;

                }

            }
            
        }

        //if we have no pending queues we have to add this to the set of idle
        //connections.
        synchronized( idleConnections ) {

            ++totalIdle;
            idleConnections.add( conn.getDelegate() );
            
            removeTrackedConnection( conn );

        }

    }

    // **** connection tracking *************************************************

    public BasicDatabaseConnection addTrackedConnection( BasicDatabaseConnection conn ) {

        if ( ! enableTracking ) 
            return conn;

        synchronized( trackedConnections ) {
            trackedConnections.put( conn.getDelegate(),
                                    new TrackedConnection( new Exception(),
                                                           conn ) );
        }
            
        return conn;
        
    }

    public BasicDatabaseConnection removeTrackedConnection( BasicDatabaseConnection conn ) {

        if ( ! enableTracking ) 
            return conn;

        synchronized( trackedConnections ) {
            trackedConnections.remove( conn.getDelegate() );
        }

        return conn;
        
    }

    public HashMap getTrackedConnections() {
        return trackedConnections;
    }

    public void dumpTrackedConnections( PrintWriter out ) {

        if ( enableTracking == false ) {

            out.println( "Connection tracking disabled for this pool." );
            
            return;
            
        } 

        HashMap connections = getTrackedConnections();
        
        out.println( "Total tracked connections: " + connections.size() );
        
        synchronized( trackedConnections ) {

            Iterator it = connections.keySet().iterator();

            while ( it.hasNext() ) {

                TrackedConnection te = (TrackedConnection)connections.get( it.next() );

                out.println( "---------" );
                out.println( "duration: " + (System.currentTimeMillis() - te.timestamp) + "ms" );

                try { 
                    
                    out.println( "isClosed: " + te.conn.isClosed() );
                    
                } catch ( Throwable t ) { }
                
                Exception e = te.exception;
                e.printStackTrace( out );

            }

        }        

    }

    public void closeTrackedConnections( PrintWriter out ) {

        if ( enableTracking == false ) {
            out.println( "Connection tracking disabled for this pool." );
            return;
        } 

        List list = new LinkedList();

        synchronized( trackedConnections ) {

            list.addAll( trackedConnections.keySet() );

        }

        out.println( "Total tracked connections: " + list.size() );
        Iterator it = list.iterator();

        while ( it.hasNext() ) {

            TrackedConnection te = (TrackedConnection)trackedConnections.get( it.next() );

            try { 

                //state that we are closing a connection... include the isClosed
                //so that I can see if there was a problem with closing it or it
                //was just abanondoned.
                
                out.println( "Closing connection: isClosed: " + te.conn.isClosed() );
                
                te.conn.close();
                    
            } catch ( Throwable t ) {
                    
                t.printStackTrace();
                    
            }
                
        }

    }
    
    // **** DataSource **********************************************************

    public Connection getConnection(String username, String password) 
        throws SQLException {

        return getConnection();

    }

    public java.io.PrintWriter getLogWriter() throws SQLException {
        return new PrintWriter( System.out );
    }

    public void setLogWriter(java.io.PrintWriter out) throws SQLException {}
    public void setLoginTimeout(int seconds) throws SQLException {}
    public int getLoginTimeout() throws SQLException {
        return 1000;
    }

    class TrackedConnection {

        public long timestamp = System.currentTimeMillis();
        public Exception exception = null;
        public BasicDatabaseConnection conn = null;

        public TrackedConnection( Exception e,
                                  BasicDatabaseConnection conn ) {
            this.exception = e;
            this.conn = conn;
        }
        
    }
    
}

/**
 * Class used for wait/notify operations which can act as a closure for the
 * resulting connection.
 */
class ConnectionMonitor {
    
    public Connection conn = null;

}