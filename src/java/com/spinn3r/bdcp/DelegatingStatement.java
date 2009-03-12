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

import java.sql.*;

import java.util.List;
import java.util.Iterator;

/**
 * A base delegating implementation of {@link Statement}.
 * <p>
 * All of the methods from the {@link Statement} interface
 * simply check to see that the {@link Statement} is active,
 * and call the corresponding method on the "delegate"
 * provided in my constructor.
 * <p>
 * Extends AbandonedTrace to implement Statement tracking and
 * logging of code which created the Statement. Tracking the
 * Statement ensures that the Connection which created it can
 * close any open Statement's on Connection close.
 *
 * @author Rodney Waldhoff (<a href="mailto:rwaldhof@us.britannica.com">rwaldhof@us.britannica.com</a>)
 * @author Glenn L. Nielsen
 * @author James House (<a href="mailto:james@interobjective.com">james@interobjective.com</a>)
 */
public class DelegatingStatement implements Statement {

    /** My delegate. */
    protected Statement _stmt = null;

    /** The connection that created me. **/
    protected BasicDatabaseConnection _conn = null;

    /**
     * Create a wrapper for the Statement which traces this
     * Statement to the Connection which created it and the
     * code which created it.
     *
     * @param s the {@link Statement} to delegate all calls to.
     * @param c the {@link DelegatingConnection} that created this statement.
     */
    public DelegatingStatement( BasicDatabaseConnection c, Statement s) {
        _stmt = s;
        _conn = c;
    }

    /**
     * Returns my underlying {@link Statement}.
     * @return my underlying {@link Statement}.
     */
    public Statement getDelegate() {
        return _stmt;
    }

    /**
     * If my underlying {@link Statement} is not a
     * <tt>DelegatingStatement</tt>, returns it,
     * otherwise recursively invokes this method on
     * my delegate.
     * <p>
     * Hence this method will return the first
     * delegate that is not a <tt>DelegatingStatement</tt>
     * or <tt>null</tt> when no non-<tt>DelegatingStatement</tt>
     * delegate can be found by transversing this chain.
     * <p>
     * This method is useful when you may have nested
     * <tt>DelegatingStatement</tt>s, and you want to make
     * sure to obtain a "genuine" {@link Statement}.
     */
    public Statement getInnermostDelegate() {
        Statement s = _stmt;
        while(s != null && s instanceof DelegatingStatement) {
            s = ((DelegatingStatement)s).getDelegate();
            if(this == s) {
                return null;
            }
        }
        return s;
    }

    /** Sets my delegate. */
    public void setDelegate(Statement s) {
        _stmt = s;
    }

    /**
     * Close this DelegatingStatement, and close
     * any ResultSets that were not explicitly closed.
     */
    public void close() throws SQLException {
        _stmt.close();
    }

    public Connection getConnection() throws SQLException {
        checkOpen();
        return _conn; // return the delegating connection that created this
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();

        return _stmt.executeQuery(sql);
    }

    public ResultSet getResultSet() throws SQLException {
        checkOpen();

        return _stmt.getResultSet();
    }

    public int executeUpdate(String sql) throws SQLException { checkOpen(); return _stmt.executeUpdate(sql);}
    public int getMaxFieldSize() throws SQLException { checkOpen(); return _stmt.getMaxFieldSize();}
    public void setMaxFieldSize(int max) throws SQLException { checkOpen(); _stmt.setMaxFieldSize(max);}
    public int getMaxRows() throws SQLException { checkOpen(); return _stmt.getMaxRows();}
    public void setMaxRows(int max) throws SQLException { checkOpen(); _stmt.setMaxRows(max);}
    public void setEscapeProcessing(boolean enable) throws SQLException { checkOpen(); _stmt.setEscapeProcessing(enable);}
    public int getQueryTimeout() throws SQLException { checkOpen(); return _stmt.getQueryTimeout();}
    public void setQueryTimeout(int seconds) throws SQLException { checkOpen(); _stmt.setQueryTimeout(seconds);}
    public void cancel() throws SQLException { checkOpen(); _stmt.cancel();}
    public SQLWarning getWarnings() throws SQLException { checkOpen(); return _stmt.getWarnings();}
    public void clearWarnings() throws SQLException { checkOpen(); _stmt.clearWarnings();}
    public void setCursorName(String name) throws SQLException { checkOpen(); _stmt.setCursorName(name);}
    public boolean execute(String sql) throws SQLException { checkOpen(); return _stmt.execute(sql);}
    public int getUpdateCount() throws SQLException { checkOpen(); return _stmt.getUpdateCount();}
    public boolean getMoreResults() throws SQLException { checkOpen(); return _stmt.getMoreResults();}
    public void setFetchDirection(int direction) throws SQLException { checkOpen(); _stmt.setFetchDirection(direction);}
    public int getFetchDirection() throws SQLException { checkOpen(); return _stmt.getFetchDirection();}
    public void setFetchSize(int rows) throws SQLException { checkOpen(); _stmt.setFetchSize(rows);}
    public int getFetchSize() throws SQLException { checkOpen(); return _stmt.getFetchSize();}
    public int getResultSetConcurrency() throws SQLException { checkOpen(); return _stmt.getResultSetConcurrency();}
    public int getResultSetType() throws SQLException { checkOpen(); return _stmt.getResultSetType();}
    public void addBatch(String sql) throws SQLException { checkOpen(); _stmt.addBatch(sql);}
    public void clearBatch() throws SQLException { checkOpen(); _stmt.clearBatch();}
    public int[] executeBatch() throws SQLException { checkOpen(); return _stmt.executeBatch();}

    protected void checkOpen() throws SQLException {
        if(_closed) {
            throw new SQLException("Connection is closed.");
        }
    }

    protected boolean _closed = false;

    // ------------------- JDBC 3.0 -----------------------------------------
    // Will be uncommented by the build process on a JDBC 3.0 system

    public boolean getMoreResults(int current) throws SQLException {
        checkOpen();
        return _stmt.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();
        return _stmt.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
        throws SQLException {
        checkOpen();
        return _stmt.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int columnIndexes[])
        throws SQLException {
        checkOpen();
        return _stmt.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String columnNames[])
        throws SQLException {
        checkOpen();
        return _stmt.executeUpdate(sql, columnNames);
    }

    public boolean execute(String sql, int autoGeneratedKeys)
        throws SQLException {
        checkOpen();
        return _stmt.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int columnIndexes[])
        throws SQLException {
        checkOpen();
        return _stmt.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String columnNames[])
        throws SQLException {
        checkOpen();
        return _stmt.execute(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        checkOpen();
        return _stmt.getResultSetHoldability();
    }

}

