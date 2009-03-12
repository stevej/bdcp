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

import com.spinn3r.log5j.*;

public class BasicDatabaseConnectionPool implements DataSource {

    private static final Logger log = Logger.getLogger();

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

    /**
     * The maximum number of active connections that can be allocated from this
     * pool at the same time, or non-positive for no limit.
     */
    public static final int DEFAULT_MAX_IDLE  = -1;
    
    public static final boolean DEFAULT_ENABLE_TRACKING  = false;

    /**
     * Wait for the pool to have a new object.  Its important for this be high
     * enough to prevent CPU thrash but short enough to check often.  In the
     * future we might want to tune this at runtime.
     */
    public static int WAIT_INTERVAL = 150;

    /**
     * The list of idle connections available for pool use by calling threads.
     */
    List<Connection> idleConnections = new LinkedList();

    /**
     * The list of current connections that are thought to be active.
     */
    HashMap trackedConnections = new HashMap();

    public int initialSize = 0;

    public int maxActive = DEFAULT_MAX_ACTIVE;

    public int maxIdle = DEFAULT_MAX_IDLE;
    
    public long  maxWait = DEFAULT_MAX_WAIT;

    public int totalActive = 0;

    public int totalIdle = 0;

    /**
     * Certain connections should be reconnected at regular intervals.
     * Specifically, master connections.  This can be used with the new lbpool
     * master handling for connecting to the right machine.
     *
     */
    public long reconnectInterval = -1;
    
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
     * Map which keeps values of when a connection needs to be reconnected.
     *
     */
    private Map<Connection, Long> reconnectConnectionTrackMap = new TreeMap();
    
    public BasicDatabaseConnectionPool() {

        ConnectionPingThread pingthread = new ConnectionPingThread( this );
        pingthread.start();

    }

    /**
     * Called AFTER we've set all necessary variables.
     */
    public void initialize() { }

    public Connection getConnection() throws SQLException {

        long duration = 0;

        // NOTE: http://en.wikipedia.org/wiki/Lamport%27s_bakery_algorithm
        //
        // Lamport's bakery algorithm might actually be more efficient here and
        // use less CPU and allow us to distributed workload more evenly when
        // the queue is exhaustd.

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

                    //borrow an object from the queue.
                    conn = idleConnections.remove( 0 );
                    
                    --totalIdle;

                    BasicDatabaseConnection bdc = new BasicDatabaseConnection( conn, this );
                    
                    return addTrackedConnection( bdc );
                    
                }

                //create a new connection as we haven't hit our limit
                if ( idleConnections.size() == 0 && totalActive < maxActive ) {

                    //FIXME: this is a BIG BUG...... createConnection requires
                    //NEW IO so we should not be performing this while
                    //synchronized around idleConnections becuase this prevents
                    //connections from being returned.  The autoReconnect=true
                    //setting in the JDBC driver will cause this to spin
                    //forever.  If a SINGLE connection blocks it can lock up the
                    //ENTIRE threadeing system.

                    BasicDatabaseConnection bdc = createConnection();
                    
                    return addTrackedConnection( bdc );
                }

            }

            //no more connections.  Wait for one.
            try { 

                Thread.sleep( WAIT_INTERVAL );
                
            } catch ( Exception e ) { /* ignore sleep issues*/ }
            
            duration += WAIT_INTERVAL;
            
            //NOTE: we will need to have actions here for when we run out of
            //duration.
            if ( maxWait > 0 && duration > maxWait )
                throw new SQLException( "Unable to obtain connection to database.  " +
                                        "Wait time exceeded: " + maxWait );
            
        }

    }

    /**
     * Physically create a new connection to the database using the JDBC
     * DriverManager.
     *
     */
    public BasicDatabaseConnection createConnection() throws SQLException {

        String message = "Took too long to connect to %s with user %s";

        long duration = 5000L;
        
        CompletionWatcher watcher = new CompletionWatcher( log , duration, message, url, user );

        try {

            watcher.start();

            Connection conn = DriverManager.getConnection( url, user, password );
            ++totalActive;
            
            return new BasicDatabaseConnection( conn, this );

        } catch ( SQLException e ) {
            log.error( "Couldn't create connection: " + url );
            throw e;
        } finally {
            watcher.complete();
        }
        
    }
        
    /**
     * Return an object to the pool for others to use.
     *
     */
    public void returnObject( BasicDatabaseConnection conn ) throws SQLException {

        /**
         * Handle closing connections at runtime when necessary so that we keep
         * the number of idle connections low and avoid wasting memory.
         */
        if ( maxIdle >= 0 && totalIdle >= maxIdle ) {

            conn.getDelegate().close();
            --totalActive;
            
            return;

        }
        
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
                    
                } catch ( Exception e ) { }
                
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
                    
            } catch ( Exception e ) {
                    
                e.printStackTrace();
                    
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
 * Track ReconnectTrackMaps and include most logic for when we should forc
 * reconnect of connections.
 *
 */
class ReconnectTrackMapManager {

    private Map<Connection,ReconnectTrackMap> instances = new TreeMap();

    private BasicDatabaseConnectionPool pool = null;

    public ReconnectTrackMapManager( BasicDatabaseConnectionPool pool ) {
        this.pool = pool;
    }
    
    /**
     * Reconnect a connection but only when necessary.  
     *
     */
    public Connection reconnectWhenNecessary( Connection conn ) {

        //cheat here for connections that don't need reconnection.  This allows
        //us to bypass all synchronized blocks
        if ( pool.reconnectInterval == -1 )
            return conn;

        ReconnectTrackMap rtp = getReconnectTrackMap( conn  );
        
        if ( ! rtp.needsReconnect() )
            return conn;

        return reconnect( conn );
        
    }

    /**
     * Get an underlying reconnect map or create a new one when necessary.
     *
     */
    ReconnectTrackMap getReconnectTrackMap( Connection conn ) {

        synchronized( instances ) {

            ReconnectTrackMap result = instances.get( conn );
            if ( result == null ) {

                result = new ReconnectTrackMap( pool );

                //this is a somewhat synthetic timestamp since it was created before.

                result.connected = System.currentTimeMillis();
                instances.put( conn, result );

            }
                
            return result;
            
        }
        
    }

    /**
     * Reconnect the given connection and return a new one.  Also, update stored
     * metadata.
     *
     */
    Connection reconnect( Connection conn ) {

        synchronized( instances ) {
            instances.remove( conn );
        }

        while( true ) {

            try {

                //only use one ReconnectTrackMap per virtual connection so that
                //we can reduce GC.
                ReconnectTrackMap rtm = instances.get( conn );

                //close out the existing connection.
                conn.close();
                
                //open the new connection
                conn = pool.createConnection();
                
                rtm.connected = System.currentTimeMillis();

                synchronized( instances ) {
                    instances.put( conn, rtm );
                }

                break;

            } catch ( Exception e ) {

                //an error has occured, we need to sleep for a second and retry.
                try { 
                    Thread.sleep( 5000 );
                } catch ( InterruptedException ie ) { break; }
                
            }

        }

        return conn;
        
    }
    
}

/**
 * Keeps track of reconnect maps and when we should reconnect.
 *
 */
class ReconnectTrackMap {

    private BasicDatabaseConnectionPool pool = null;

    public ReconnectTrackMap( BasicDatabaseConnectionPool pool ) {
        this.pool = pool;
    }

    /**
     * The time in milliseconds that this connection was established.
     */
    public long connected = -1;

    /**
     * Return true if we need to reconnect this connection.
     *
     */
    boolean needsReconnect() {

        if ( connected == 0 )
            return false;
        
        return System.currentTimeMillis() - connected > pool.reconnectInterval;
        
    }

}