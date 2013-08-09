/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.sql;

import java.io.*;
import java.sql.*;
import org.h2.jdbcx.JdbcDataSource;

import org.pickle.logging.Log;
import org.pickle.Disposable;

/**
 * This class creates or opens an H2 database and provides access to a JDBC Connection to the database.
 * The connection is automatically closed on finalize(), but calling dispose() is preferable.
 */
public class ConnectionManager implements Disposable {

  private String jdbcUrl;
  private Connection connection;
  
  /**
   * Opens an H2 database located in dataDir.  If the database does not exist, it is created.  The createSql
   * script is executed each time, so it must insure SQL uses statements such as "IF NOT EXISTS" where
   * appropriate.
   * @param dataDir the base directory of the database
   * @param createSql the SQL creation script
   */
  public ConnectionManager(File dataDir, String createSql) {
    if (!dataDir.exists() || !dataDir.isDirectory() || !dataDir.canWrite()) {
      throw new DatabaseException(
          String.format("'%s' is not a writable directory or does not exist.", dataDir));
    }
    else {
      jdbcUrl = String.format("jdbc:h2:%s/pickle", dataDir.getAbsolutePath());
      try {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(jdbcUrl);
        ds.setUser("sa");
        ds.setPassword("");
        connection = ds.getConnection();
        Log.info("Opened database connection to '%s'.", jdbcUrl);
        connection.setAutoCommit(false);
        JdbcTemplate.executeUpdate(connection, createSql);
      }
      catch (SQLException e) {
        throw new DatabaseException("Unable to connect to H2 database at: " + jdbcUrl, e);
      }
    }
  }
  
  /**
   * Returns a JDBC Connection to the H2 database.
   * @return the Connection
   */
  public Connection getConnection() {
    return connection;
  }
  
  /**
   * Closes the JDBC Connection.  This ConnectionManager becomes unusable.
   */
  public void dispose() {
    try {
      if (!connection.isClosed()) {
        connection.close();
        Log.info("Closed database connection to '%s'.", jdbcUrl);
      }
    }
    catch (SQLException e) {
      Log.warn("Failed to close database connection to '%s'.", jdbcUrl);
    }
  }
  
  @Override
  protected void finalize() throws Throwable {
    if (connection != null) {
      try {
        dispose();
      }
      finally {
        super.finalize();
      }
    }
  }
}