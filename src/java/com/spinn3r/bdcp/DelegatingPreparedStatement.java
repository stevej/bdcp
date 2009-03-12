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
import java.util.Calendar;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Iterator;

/**
 * A base delegating implementation of {@link PreparedStatement}.
 * <p>
 * All of the methods from the {@link PreparedStatement} interface
 * simply check to see that the {@link PreparedStatement} is active,
 * and call the corresponding method on the "delegate"
 * provided in my constructor.
 * <p>
 * Extends AbandonedTrace to implement Statement tracking and
 * logging of code which created the Statement. Tracking the 
 * Statement ensures that the Connection which created it can
 * close any open Statement's on Connection close.
 *
 * @author Rodney Waldhoff
 * @author Glenn L. Nielsen
 * @author James House (<a href="mailto:james@interobjective.com">james@interobjective.com</a>)
 */
public class DelegatingPreparedStatement implements PreparedStatement {
    /** My delegate. */
    protected PreparedStatement _stmt = null;
    /** The connection that created me. **/
    protected BasicDatabaseConnection _conn = null;

    protected boolean _closed = false;

    /**
     * Create a wrapper for the Statement which traces this
     * Statement to the Connection which created it and the
     * code which created it.
     *
     * @param s the {@link PreparedStatement} to delegate all calls to.
     * @param c the {@link DelegatingConnection} that created this statement.
     */
    public DelegatingPreparedStatement( BasicDatabaseConnection c,
                                        PreparedStatement s ) {
        _stmt = s;
        _conn = c;
    }

    /**
     * Returns my underlying {@link PreparedStatement}.
     * @return my underlying {@link PreparedStatement}.
     */
    public PreparedStatement getDelegate() {
        return _stmt;
    }

    /**
     * If my underlying {@link PreparedStatement} is not a
     * <tt>DelegatingPreparedStatement</tt>, returns it,
     * otherwise recursively invokes this method on
     * my delegate.
     * <p>
     * Hence this method will return the first
     * delegate that is not a <tt>DelegatingPreparedStatement</tt>,
     * or <tt>null</tt> when no non-<tt>DelegatingPreparedStatement</tt>
     * delegate can be found by transversing this chain.
     * <p>
     * This method is useful when you may have nested
     * <tt>DelegatingPreparedStatement</tt>s, and you want to make
     * sure to obtain a "genuine" {@link PreparedStatement}.
     */
    public PreparedStatement getInnermostDelegate() {
        PreparedStatement s = _stmt;
        while(s != null && s instanceof DelegatingPreparedStatement) {
            s = ((DelegatingPreparedStatement)s).getDelegate();
            if(this == s) {
                return null;
            }
        }
        return s;
    }

    /** Sets my delegate. */
    public void setDelegate(PreparedStatement s) {
        _stmt = s;
    }

    /**
     * Close this DelegatingPreparedStatement, and close
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

    public ResultSet executeQuery() throws SQLException {
        checkOpen();

        return _stmt.executeQuery();
    }

    public int executeUpdate(String sql) throws SQLException { checkOpen(); return _stmt.executeUpdate(sql);}
    public int getMaxFieldSize() throws SQLException { checkOpen(); return _stmt.getMaxFieldSize();}
    public void setMaxFieldSize(int max) throws SQLException { checkOpen();_stmt.setMaxFieldSize(max);}
    public int getMaxRows() throws SQLException { checkOpen();return _stmt.getMaxRows();}
    public void setMaxRows(int max) throws SQLException { checkOpen(); _stmt.setMaxRows(max);}
    public void setEscapeProcessing(boolean enable) throws SQLException { checkOpen();_stmt.setEscapeProcessing(enable);}
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

    public int executeUpdate() throws SQLException { checkOpen(); return _stmt.executeUpdate();}
    public void setNull(int parameterIndex, int sqlType) throws SQLException { checkOpen(); _stmt.setNull(parameterIndex,sqlType);}
    public void setBoolean(int parameterIndex, boolean x) throws SQLException { checkOpen(); _stmt.setBoolean(parameterIndex,x);}
    public void setByte(int parameterIndex, byte x) throws SQLException { checkOpen(); _stmt.setByte(parameterIndex,x);}
    public void setShort(int parameterIndex, short x) throws SQLException { checkOpen(); _stmt.setShort(parameterIndex,x);}
    public void setInt(int parameterIndex, int x) throws SQLException { checkOpen(); _stmt.setInt(parameterIndex,x);}
    public void setLong(int parameterIndex, long x) throws SQLException { checkOpen(); _stmt.setLong(parameterIndex,x);}
    public void setFloat(int parameterIndex, float x) throws SQLException { checkOpen(); _stmt.setFloat(parameterIndex,x);}
    public void setDouble(int parameterIndex, double x) throws SQLException { checkOpen(); _stmt.setDouble(parameterIndex,x);}
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException { checkOpen(); _stmt.setBigDecimal(parameterIndex,x);}
    public void setString(int parameterIndex, String x) throws SQLException { checkOpen(); _stmt.setString(parameterIndex,x);}
    public void setBytes(int parameterIndex, byte x[]) throws SQLException { checkOpen(); _stmt.setBytes(parameterIndex,x);}
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException { checkOpen(); _stmt.setDate(parameterIndex,x);}
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException { checkOpen(); _stmt.setTime(parameterIndex,x);}
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException { checkOpen(); _stmt.setTimestamp(parameterIndex,x);}
    public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException { checkOpen(); _stmt.setAsciiStream(parameterIndex,x,length);}
    /** @deprecated */
    public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException { checkOpen(); _stmt.setUnicodeStream(parameterIndex,x,length);}
    public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException{ checkOpen(); _stmt.setBinaryStream(parameterIndex,x,length);}
    public void clearParameters() throws SQLException { checkOpen(); _stmt.clearParameters();}
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException { checkOpen(); _stmt.setObject(parameterIndex, x, targetSqlType, scale);}
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException { checkOpen(); _stmt.setObject(parameterIndex, x, targetSqlType);}
    public void setObject(int parameterIndex, Object x) throws SQLException { checkOpen(); _stmt.setObject(parameterIndex, x);}
    public boolean execute() throws SQLException { checkOpen(); return _stmt.execute();}
    public void addBatch() throws SQLException { checkOpen(); _stmt.addBatch();}
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException { checkOpen(); _stmt.setCharacterStream(parameterIndex,reader,length);}
    public void setRef (int i, Ref x) throws SQLException { checkOpen(); _stmt.setRef(i,x);}
    public void setBlob (int i, Blob x) throws SQLException { checkOpen(); _stmt.setBlob(i,x);}
    public void setClob (int i, Clob x) throws SQLException { checkOpen(); _stmt.setClob(i,x);}
    public void setArray (int i, Array x) throws SQLException { checkOpen(); _stmt.setArray(i,x);}
    public ResultSetMetaData getMetaData() throws SQLException { checkOpen(); return _stmt.getMetaData();}
    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) throws SQLException { checkOpen(); _stmt.setDate(parameterIndex,x,cal);}
    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) throws SQLException { checkOpen(); _stmt.setTime(parameterIndex,x,cal);}
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException { checkOpen(); _stmt.setTimestamp(parameterIndex,x,cal);}
    public void setNull (int paramIndex, int sqlType, String typeName) throws SQLException { checkOpen(); _stmt.setNull(paramIndex,sqlType,typeName);}

    protected void checkOpen() throws SQLException {
        if(_closed) {
            throw new SQLException("Connection is closed.");
        }
    }

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

    public void setURL(int parameterIndex, java.net.URL x)
        throws SQLException {
        checkOpen();
        _stmt.setURL(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        return _stmt.getParameterMetaData();
    }

}
