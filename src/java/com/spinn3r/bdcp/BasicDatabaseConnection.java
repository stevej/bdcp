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
import java.io.PrintWriter;
import javax.sql.DataSource;
import java.util.Enumeration;

/**
 * A simple {@link DataSource} implementation that obtains
 * {@link Connection}s from the specified {@link ObjectPool}.
 *
 * @author Rodney Waldhoff
 * @author Glenn L. Nielsen
 * @author James House (<a href="mailto:james@interobjective.com">james@interobjective.com</a>)
 * @version $Id: BasicDatabaseConnection.java,v 1.4 2005/05/26 02:52:57 burton Exp $
 */
public class BasicDatabaseConnection implements Connection {

    //The real connection interface...
    Connection _conn = null;
    BasicDatabaseConnectionPool pool = null;
    
    boolean _closed = false;

    public BasicDatabaseConnection( Connection conn,
                                    BasicDatabaseConnectionPool pool ) {
        this._conn = conn;
        this.pool = pool;

    }

    public String toString() {
        return _conn.toString();
    }

    public void close() throws SQLException
    {

        if ( _closed == false ) {
            
            pool.returnObject( this );

        }

        _closed = true;

        //return this bad boy to the pool

    }

    protected void checkOpen() throws SQLException {

        if( _closed ) {
            throw new SQLException("Connection is closed.");
        }

    }

    /**
     * Get the internal connection.  Use for debug purposes only.
     */
    public Connection getDelegate() {
        return _conn;
    }
    
    // **** java.sql.Connection *************************************************

    public PreparedStatement prepareStatement(String sql) throws SQLException {

        checkOpen();
        return new DelegatingPreparedStatement( this, _conn.prepareStatement(sql) );
    }

    public Statement createStatement() throws SQLException {

        checkOpen();

        return new DelegatingStatement( this, _conn.createStatement() );
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        checkOpen();
        return new DelegatingCallableStatement( this, _conn.prepareCall( sql ) );
    }

    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return _conn.nativeSQL( sql );
    }

    public PreparedStatement prepareStatement( String sql, String columnNames[] )
        throws SQLException {

        checkOpen();
        return new DelegatingPreparedStatement( this, _conn.prepareStatement(sql, columnNames) );

    }

    public java.sql.Savepoint setSavepoint() throws SQLException {
        checkOpen();
        return _conn.setSavepoint();
    }

    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        return _conn.setSavepoint(name);
    }

    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
        throws SQLException {
        checkOpen();
        return new DelegatingPreparedStatement( this,_conn.prepareStatement(sql, columnIndexes));
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
        checkOpen();
        return new DelegatingPreparedStatement( this, _conn.prepareStatement(sql, autoGeneratedKeys) );
    }
    
    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability)
        throws SQLException {
        checkOpen();
        return new DelegatingPreparedStatement( this,_conn.prepareStatement(sql,
                                                                            resultSetType,
                                                                            resultSetConcurrency,
                                                                            resultSetHoldability) );
    }

    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency)
        throws SQLException {
        checkOpen();

        return new DelegatingPreparedStatement( this, _conn.prepareStatement( sql,
                                                                              resultSetType,
                                                                              resultSetConcurrency ) );
    }

    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency)
            throws SQLException {
        checkOpen();

        return new DelegatingCallableStatement
            (this, _conn.prepareCall(sql, resultSetType,resultSetConcurrency));
    }

    public boolean isReadOnly() throws SQLException { checkOpen(); return _conn.isReadOnly();}
    public void rollback() throws SQLException { checkOpen(); _conn.rollback();}
    public void setAutoCommit(boolean autoCommit) throws SQLException { checkOpen(); _conn.setAutoCommit(autoCommit);}
    public void setCatalog(String catalog) throws SQLException { checkOpen(); _conn.setCatalog(catalog);}
    public void setReadOnly(boolean readOnly) throws SQLException { checkOpen(); _conn.setReadOnly(readOnly);}
    public void setTransactionIsolation(int level) throws SQLException { checkOpen(); _conn.setTransactionIsolation(level);}
    public void setTypeMap(Map map) throws SQLException { checkOpen(); _conn.setTypeMap(map);}

    public void clearWarnings() throws SQLException { checkOpen(); _conn.clearWarnings();}

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability)
        throws SQLException {
        checkOpen();
        return new DelegatingCallableStatement( this, _conn.prepareCall(sql, resultSetType,
                                                                        resultSetConcurrency,
                                                                        resultSetHoldability ) );
    }

    public Map getTypeMap() throws SQLException { checkOpen(); return _conn.getTypeMap();}

    public int getHoldability() throws SQLException {
        checkOpen();
        return _conn.getHoldability();
    }

    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        _conn.setHoldability(holdability);
    }

    public void rollback(java.sql.Savepoint savepoint) throws SQLException {
        checkOpen();
        _conn.rollback(savepoint);
    }

    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
        checkOpen();
        _conn.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability)
        throws SQLException {
        checkOpen();
        return new DelegatingStatement( this, _conn.createStatement( resultSetType,
                                                                     resultSetConcurrency,
                                                                     resultSetHoldability ) );
    }

    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency)
            throws SQLException {
        checkOpen();

        return new DelegatingStatement
            (this, _conn.createStatement(resultSetType,resultSetConcurrency));
    }

    public SQLWarning getWarnings() throws SQLException { checkOpen(); return _conn.getWarnings();}
    public DatabaseMetaData getMetaData() throws SQLException { checkOpen(); return _conn.getMetaData();}
    public String getCatalog() throws SQLException { checkOpen(); return _conn.getCatalog();}
    public boolean getAutoCommit() throws SQLException { checkOpen(); return _conn.getAutoCommit();}

    public boolean isClosed() throws SQLException {

        if( _closed || _conn.isClosed() ) 
             return true;

         return false;

    }

    public void commit() throws SQLException { checkOpen(); _conn.commit();}

    public int getTransactionIsolation() throws SQLException { checkOpen(); return _conn.getTransactionIsolation();}

}