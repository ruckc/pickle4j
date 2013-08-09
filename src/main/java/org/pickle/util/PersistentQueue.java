/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.util.*;
import java.io.*;
import java.sql.*;
import org.pickle.Disposable;
import org.pickle.logging.Log;
import org.pickle.sql.*;

public class PersistentQueue<E extends Serializable>
             extends AbstractQueue<E> implements Disposable {
  
  static final class SQL {
    public static final String CREATE_TABLE =
        "CREATE TABLE IF NOT EXISTS QUEUE (ID IDENTITY PRIMARY KEY, OBJECT OTHER NOT NULL)";
    
    public static final String INSERT_OBJECT = "INSERT INTO QUEUE (OBJECT) VALUES (?)";
  
    public static final String SELECT_OLDEST_OBJECT =
        "SELECT ID, OBJECT FROM QUEUE WHERE ID = (SELECT MIN(ID) FROM QUEUE)";
      
    public static final String SELECT_NEXT_ID = "SELECT MIN(ID) FROM (SELECT ID FROM QUEUE WHERE ID > ?)";
  
    public static final String DELETE_OLDEST_OBJECT =
        "DELETE FROM QUEUE WHERE ID = (SELECT MIN(ID) FROM QUEUE)";
  
    public static final String COUNT_OBJECTS = "SELECT COUNT(ID) FROM QUEUE";
  
    public static final String SELECT_ID = "SELECT ID FROM QUEUE WHERE ID = ?";
  
    public static final String SELECT_OBJECT = "SELECT OBJECT FROM QUEUE WHERE ID = ?";
  
    public static final String DELETE_OBJECT = "DELETE FROM QUEUE WHERE ID = ?";
  }

  private ConnectionManager cm;
  
  public PersistentQueue(File dataDir) {
    cm = new ConnectionManager(dataDir, SQL.CREATE_TABLE);
  }
  
  public void dispose() {
    cm.dispose();
    cm = null;
  }

  public synchronized boolean offer(final E object) {
    if (object == null) {
      throw new IllegalArgumentException("null is not supported.");
    }   
    JdbcTemplate.executeUpdate(cm.getConnection(), SQL.INSERT_OBJECT, new JdbcTemplate() {
      public void statement(PreparedStatement ps) throws SQLException {
        ps.setObject(1, object, Types.JAVA_OBJECT);
      }
    });
    return true;
  }
  
  @SuppressWarnings("unchecked")
  public synchronized E peek() {
    return (E) JdbcTemplate.executeQuery(cm.getConnection(), SQL.SELECT_OLDEST_OBJECT, new JdbcTemplate() {
      public Object results(ResultSet rs) throws SQLException {
        return rs.next() ? rs.getObject(2) : null;
      }
    });
  }
  
  public synchronized E poll() {
    E object = peek();
    if (object != null) {
      JdbcTemplate.executeUpdate(cm.getConnection(), SQL.DELETE_OLDEST_OBJECT);
    }
    return object;
  }
  
  public synchronized int size() {
    // TODO: The queue size can probably be tracked and stored like PersistentBlockingQueue.
    //       Then this query would only need to be called upon construction.
    return (Integer) JdbcTemplate.executeQuery(cm.getConnection(), SQL.COUNT_OBJECTS, new JdbcTemplate() {
      public Object results(ResultSet rs) throws SQLException {
        return rs.next() ? rs.getInt(1) : 0;
      }
    });
  }
  
  public Iterator<E> iterator() {
    return new PersistentQueueIterator<E>(cm);
  }

  /**
   * Executes a block of code within a transaction associated with this PersistentQueue.
   * If the operator is run without an exception, the transaction is committed.  If the
   * operator throws a RuntimeException, the transaction is rolled back.
   * @param operator the Runnable containing the code to run within a transaction
   */
  public void inTransaction(Runnable operator) {
    JdbcTemplate.openTransaction(cm);
    try {
      operator.run();
      JdbcTemplate.commitTransaction();
    }
    catch (RuntimeException e) {
      Log.debug("Rolling back transaction due to exception", e);
      JdbcTemplate.rollbackTransaction();
    }
  }
}