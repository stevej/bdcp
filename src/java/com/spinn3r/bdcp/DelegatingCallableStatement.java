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

import java.net.URL;
import java.sql.CallableStatement;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.sql.Ref;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Array;
import java.util.Calendar;
import java.sql.ResultSet;
import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.List;
import java.util.Iterator;

/**
 * A base delegating implementation of {@link CallableStatement}.
 * <p>
 * All of the methods from the {@link CallableStatement} interface
 * simply call the corresponding method on the "delegate"
 * provided in my constructor.
 * <p>
 * Extends AbandonedTrace to implement Statement tracking and
 * logging of code which created the Statement. Tracking the
 * Statement ensures that the Connection which created it can
 * close any open Statement's on Connection close.
 *
 * @author Glenn L. Nielsen
 * @author James House (<a href="mailto:james@interobjective.com">james@interobjective.com</a>)
 */
public class DelegatingCallableStatement implements CallableStatement {

    /** My delegate. */
    protected CallableStatement _stmt = null;

    /** The connection that created me. **/
    protected BasicDatabaseConnection _conn = null;

    /**
     * Create a wrapper for the Statement which traces this
     * Statement to the Connection which created it and the
     * code which created it.
     *
     * @param cs the {@link CallableStatement} to delegate all calls to.
     */
    public DelegatingCallableStatement( BasicDatabaseConnection c,
                                        CallableStatement s ) {
        _conn = c;
        _stmt = s;
    }

    /**
     * Close this DelegatingCallableStatement, and close
     * any ResultSets that were not explicitly closed.
     */
    public void close() throws SQLException {

        _stmt.close();

    }

    public Connection getConnection() throws SQLException {
      return _conn;
    }

    public ResultSet executeQuery() throws SQLException {
        return _stmt.executeQuery();
    }

    public ResultSet getResultSet() throws SQLException {
        return _stmt.getResultSet();
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return _stmt.executeQuery(sql);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException { _stmt.registerOutParameter( parameterIndex,  sqlType);  }
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException { _stmt.registerOutParameter( parameterIndex,  sqlType,  scale);  }
    public boolean wasNull() throws SQLException { return _stmt.wasNull();  }
    public String getString(int parameterIndex) throws SQLException { return _stmt.getString( parameterIndex);  }
    public boolean getBoolean(int parameterIndex) throws SQLException { return _stmt.getBoolean( parameterIndex);  }
    public byte getByte(int parameterIndex) throws SQLException { return _stmt.getByte( parameterIndex);  }
    public short getShort(int parameterIndex) throws SQLException { return _stmt.getShort( parameterIndex);  }
    public int getInt(int parameterIndex) throws SQLException { return _stmt.getInt( parameterIndex);  }
    public long getLong(int parameterIndex) throws SQLException { return _stmt.getLong( parameterIndex);  }
    public float getFloat(int parameterIndex) throws SQLException { return _stmt.getFloat( parameterIndex);  }
    public double getDouble(int parameterIndex) throws SQLException { return _stmt.getDouble( parameterIndex);  }
    /** @deprecated */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException { return _stmt.getBigDecimal( parameterIndex,  scale);  }
    public byte[] getBytes(int parameterIndex) throws SQLException { return _stmt.getBytes( parameterIndex);  }
    public Date getDate(int parameterIndex) throws SQLException { return _stmt.getDate( parameterIndex);  }
    public Time getTime(int parameterIndex) throws SQLException { return _stmt.getTime( parameterIndex);  }
    public Timestamp getTimestamp(int parameterIndex) throws SQLException { return _stmt.getTimestamp( parameterIndex);  }
    public Object getObject(int parameterIndex) throws SQLException { return _stmt.getObject( parameterIndex);  }
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException { return _stmt.getBigDecimal( parameterIndex);  }
    public Object getObject(int i, Map map) throws SQLException { return _stmt.getObject( i, map);  }
    public Ref getRef(int i) throws SQLException { return _stmt.getRef( i);  }
    public Blob getBlob(int i) throws SQLException { return _stmt.getBlob( i);  }
    public Clob getClob(int i) throws SQLException { return _stmt.getClob( i);  }
    public Array getArray(int i) throws SQLException { return _stmt.getArray( i);  }
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException { return _stmt.getDate( parameterIndex,  cal);  }
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException { return _stmt.getTime( parameterIndex,  cal);  }
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException { return _stmt.getTimestamp( parameterIndex,  cal);  }
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException { _stmt.registerOutParameter( paramIndex,  sqlType,  typeName);  }
    public int executeUpdate() throws SQLException { return _stmt.executeUpdate();  }
    public void setNull(int parameterIndex, int sqlType) throws SQLException { _stmt.setNull( parameterIndex,  sqlType);  }
    public void setBoolean(int parameterIndex, boolean x) throws SQLException { _stmt.setBoolean( parameterIndex,  x);  }
    public void setByte(int parameterIndex, byte x) throws SQLException { _stmt.setByte( parameterIndex,  x);  }
    public void setShort(int parameterIndex, short x) throws SQLException { _stmt.setShort( parameterIndex,  x);  }
    public void setInt(int parameterIndex, int x) throws SQLException { _stmt.setInt( parameterIndex,  x);  }
    public void setLong(int parameterIndex, long x) throws SQLException { _stmt.setLong( parameterIndex,  x);  }
    public void setFloat(int parameterIndex, float x) throws SQLException { _stmt.setFloat( parameterIndex,  x);  }
    public void setDouble(int parameterIndex, double x) throws SQLException { _stmt.setDouble( parameterIndex,  x);  }
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException { _stmt.setBigDecimal( parameterIndex,  x);  }
    public void setString(int parameterIndex, String x) throws SQLException { _stmt.setString( parameterIndex,  x);  }
    public void setBytes(int parameterIndex, byte[] x) throws SQLException { _stmt.setBytes( parameterIndex,  x);  }
    public void setDate(int parameterIndex, Date x) throws SQLException { _stmt.setDate( parameterIndex,  x);  }
    public void setTime(int parameterIndex, Time x) throws SQLException { _stmt.setTime( parameterIndex,  x);  }
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException { _stmt.setTimestamp( parameterIndex,  x);  }
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException { _stmt.setAsciiStream( parameterIndex,  x,  length);  }
    /** @deprecated */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException { _stmt.setUnicodeStream( parameterIndex,  x,  length);  }
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException { _stmt.setBinaryStream( parameterIndex,  x,  length);  }
    public void clearParameters() throws SQLException { _stmt.clearParameters();  }
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException { _stmt.setObject( parameterIndex,  x,  targetSqlType,  scale);  }
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException { _stmt.setObject( parameterIndex,  x,  targetSqlType);  }
    public void setObject(int parameterIndex, Object x) throws SQLException { _stmt.setObject( parameterIndex,  x);  }
    public boolean execute() throws SQLException { return _stmt.execute();  }
    public void addBatch() throws SQLException { _stmt.addBatch();  }
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException { _stmt.setCharacterStream( parameterIndex,  reader,  length);  }
    public void setRef(int i, Ref x) throws SQLException { _stmt.setRef( i,  x);  }
    public void setBlob(int i, Blob x) throws SQLException { _stmt.setBlob( i,  x);  }
    public void setClob(int i, Clob x) throws SQLException { _stmt.setClob( i,  x);  }
    public void setArray(int i, Array x) throws SQLException { _stmt.setArray( i,  x);  }
    public ResultSetMetaData getMetaData() throws SQLException { return _stmt.getMetaData();  }
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { _stmt.setDate( parameterIndex,  x,  cal);  }
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { _stmt.setTime( parameterIndex,  x,  cal);  }
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException { _stmt.setTimestamp( parameterIndex,  x,  cal);  }
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException { _stmt.setNull( paramIndex,  sqlType,  typeName);  }

    public int executeUpdate(String sql) throws SQLException { return _stmt.executeUpdate( sql);  }
    public int getMaxFieldSize() throws SQLException { return _stmt.getMaxFieldSize();  }
    public void setMaxFieldSize(int max) throws SQLException { _stmt.setMaxFieldSize( max);  }
    public int getMaxRows() throws SQLException { return _stmt.getMaxRows();  }
    public void setMaxRows(int max) throws SQLException { _stmt.setMaxRows( max);  }
    public void setEscapeProcessing(boolean enable) throws SQLException { _stmt.setEscapeProcessing( enable);  }
    public int getQueryTimeout() throws SQLException { return _stmt.getQueryTimeout();  }
    public void setQueryTimeout(int seconds) throws SQLException { _stmt.setQueryTimeout( seconds);  }
    public void cancel() throws SQLException { _stmt.cancel();  }
    public SQLWarning getWarnings() throws SQLException { return _stmt.getWarnings();  }
    public void clearWarnings() throws SQLException { _stmt.clearWarnings();  }
    public void setCursorName(String name) throws SQLException { _stmt.setCursorName( name);  }
    public boolean execute(String sql) throws SQLException { return _stmt.execute( sql);  }


    public int getUpdateCount() throws SQLException { return _stmt.getUpdateCount();  }
    public boolean getMoreResults() throws SQLException { return _stmt.getMoreResults();  }
    public void setFetchDirection(int direction) throws SQLException { _stmt.setFetchDirection( direction);  }
    public int getFetchDirection() throws SQLException { return _stmt.getFetchDirection();  }
    public void setFetchSize(int rows) throws SQLException { _stmt.setFetchSize( rows);  }
    public int getFetchSize() throws SQLException { return _stmt.getFetchSize();  }
    public int getResultSetConcurrency() throws SQLException { return _stmt.getResultSetConcurrency();  }
    public int getResultSetType() throws SQLException { return _stmt.getResultSetType();  }
    public void addBatch(String sql) throws SQLException { _stmt.addBatch( sql);  }
    public void clearBatch() throws SQLException { _stmt.clearBatch();  }
    public int[] executeBatch() throws SQLException { return _stmt.executeBatch();  }

    // ------------------- JDBC 3.0 -----------------------------------------
    // Will be uncommented by the build process on a JDBC 3.0 system

    public boolean getMoreResults(int current) throws SQLException {
        return _stmt.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return _stmt.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
        throws SQLException {
        return _stmt.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int columnIndexes[])
        throws SQLException {
        return _stmt.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String columnNames[])
        throws SQLException {
        return _stmt.executeUpdate(sql, columnNames);
    }

    public boolean execute(String sql, int autoGeneratedKeys)
        throws SQLException {
        return _stmt.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int columnIndexes[])
        throws SQLException {
        return _stmt.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String columnNames[])
        throws SQLException {
        return _stmt.execute(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        return _stmt.getResultSetHoldability();
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        _stmt.setURL(parameterIndex, x);
    }

    public java.sql.ParameterMetaData getParameterMetaData()
        throws SQLException {
        return _stmt.getParameterMetaData();
    }

    public void registerOutParameter(String parameterName, int sqlType)
        throws SQLException {
        _stmt.registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName,
        int sqlType, int scale) throws SQLException {
        _stmt.registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName,
        int sqlType, String typeName) throws SQLException {
        _stmt.registerOutParameter(parameterName, sqlType, typeName);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return _stmt.getURL(parameterIndex);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        _stmt.setURL(parameterName, val);
    }

    public void setNull(String parameterName, int sqlType)
        throws SQLException {
        _stmt.setNull(parameterName, sqlType);
    }

    public void setBoolean(String parameterName, boolean x)
        throws SQLException {
        _stmt.setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x)
        throws SQLException {
        _stmt.setByte(parameterName, x);
    }

    public void setShort(String parameterName, short x)
        throws SQLException {
        _stmt.setShort(parameterName, x);
    }

    public void setInt(String parameterName, int x)
        throws SQLException {
        _stmt.setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x)
        throws SQLException {
        _stmt.setLong(parameterName, x);
    }

    public void setFloat(String parameterName, float x)
        throws SQLException {
        _stmt.setFloat(parameterName, x);
    }

    public void setDouble(String parameterName, double x)
        throws SQLException {
        _stmt.setDouble(parameterName, x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x)
        throws SQLException {
        _stmt.setBigDecimal(parameterName, x);
    }

    public void setString(String parameterName, String x)
        throws SQLException {
        _stmt.setString(parameterName, x);
    }

    public void setBytes(String parameterName, byte [] x)
        throws SQLException {
        _stmt.setBytes(parameterName, x);
    }

    public void setDate(String parameterName, Date x)
        throws SQLException {
        _stmt.setDate(parameterName, x);
    }

    public void setTime(String parameterName, Time x)
        throws SQLException {
        _stmt.setTime(parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x)
        throws SQLException {
        _stmt.setTimestamp(parameterName, x);
    }

    public void setAsciiStream(String parameterName,
        InputStream x, int length)
        throws SQLException {
        _stmt.setAsciiStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName,
        InputStream x, int length)
        throws SQLException {
        _stmt.setBinaryStream(parameterName, x, length);
    }

    public void setObject(String parameterName,
        Object x, int targetSqlType, int scale)
        throws SQLException {
        _stmt.setObject(parameterName, x, targetSqlType, scale);
    }

    public void setObject(String parameterName,
        Object x, int targetSqlType)
        throws SQLException {
        _stmt.setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x)
        throws SQLException {
        _stmt.setObject(parameterName, x);
    }

    public void setCharacterStream(String parameterName,
        Reader reader, int length) throws SQLException {
        _stmt.setCharacterStream(parameterName, reader, length);
    }

    public void setDate(String parameterName,
        Date x, Calendar cal) throws SQLException {
        _stmt.setDate(parameterName, x, cal);
    }

    public void setTime(String parameterName,
        Time x, Calendar cal) throws SQLException {
        _stmt.setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName,
        Timestamp x, Calendar cal) throws SQLException {
        _stmt.setTimestamp(parameterName, x, cal);
    }

    public void setNull(String parameterName,
        int sqlType, String typeName) throws SQLException {
        _stmt.setNull(parameterName, sqlType, typeName);
    }

    public String getString(String parameterName) throws SQLException {
        return _stmt.getString(parameterName);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return _stmt.getBoolean(parameterName);
    }

    public byte getByte(String parameterName) throws SQLException {
        return _stmt.getByte(parameterName);
    }

    public short getShort(String parameterName) throws SQLException {
        return _stmt.getShort(parameterName);
    }

    public int getInt(String parameterName) throws SQLException {
        return _stmt.getInt(parameterName);
    }

    public long getLong(String parameterName) throws SQLException {
        return _stmt.getLong(parameterName);
    }

    public float getFloat(String parameterName) throws SQLException {
        return _stmt.getFloat(parameterName);
    }

    public double getDouble(String parameterName) throws SQLException {
        return _stmt.getDouble(parameterName);
    }

    public byte [] getBytes(String parameterName) throws SQLException {
        return _stmt.getBytes(parameterName);
    }

    public Date getDate(String parameterName) throws SQLException {
        return _stmt.getDate(parameterName);
    }

    public Time getTime(String parameterName) throws SQLException {
        return _stmt.getTime(parameterName);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return _stmt.getTimestamp(parameterName);
    }

    public Object getObject(String parameterName) throws SQLException {
        return _stmt.getObject(parameterName);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return _stmt.getBigDecimal(parameterName);
    }

    public Object getObject(String parameterName, Map map)
        throws SQLException {
        return _stmt.getObject(parameterName, map);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return _stmt.getRef(parameterName);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return _stmt.getBlob(parameterName);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return _stmt.getClob(parameterName);
    }

    public Array getArray(String parameterName) throws SQLException {
        return _stmt.getArray(parameterName);
    }

    public Date getDate(String parameterName, Calendar cal)
        throws SQLException {
        return _stmt.getDate(parameterName, cal);
    }

    public Time getTime(String parameterName, Calendar cal)
        throws SQLException {
        return _stmt.getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal)
        throws SQLException {
        return _stmt.getTimestamp(parameterName, cal);
    }

    public URL getURL(String parameterName) throws SQLException {
        return _stmt.getURL(parameterName);
    }

}
