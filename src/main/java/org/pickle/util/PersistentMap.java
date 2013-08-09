/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.util.*;
import java.io.*;
import java.sql.*;
import org.pickle.Disposable;
import org.pickle.sql.*;

/**
 * TODO: Finish this class.
 */
public class PersistentMap<K extends Serializable, V extends Serializable>
             extends AbstractMap<K, V> implements Disposable {
  
  private static final String CREATE_TABLE_SQL =
      "CREATE TABLE IF NOT EXISTS MAP (" +
      "  ID            IDENTITY  PRIMARY KEY," +
      "  KEY_HASHCODE  INT       NOT NULL," +
      "  KEY           OTHER     NOT NULL," +
      "  VALUE         OTHER     NOT NULL)";
  
  private static final String INSERT_ENTRY_SQL = "INSERT INTO MAP (KEY_HASHCODE, KEY, VALUE) VALUES (?, ?, ?)";

  private static final String UPDATE_ENTRY_SQL = "UPDATE MAP SET VALUE = ? WHERE ID = ?";
  
  private static final String SELECT_ENTRY_BY_KEY_HASHCODE_SQL =
    "SELECT ID, KEY, VALUE FROM MAP WHERE KEY_HASHCODE = ?";
  
  private static final String COUNT_ENTRIES_BY_KEY_HASHCODE_SQL = "SELECT COUNT(ID) FROM MAP WHERE KEY_HASHCODE = ?";
  
  private static final String COUNT_ENTRIES_SQL = "SELECT COUNT(ID) FROM MAP";
  
  private ConnectionManager cm;
  
  public PersistentMap(File dataDir) {
    cm = new ConnectionManager(dataDir, CREATE_TABLE_SQL);
  }
  
  public void dispose() {
    cm.dispose();
  }
  
  @Override
  public int size() {
    // TODO: The map size can probably be tracked and stored internally.
    //       Then this query would only need to be called upon construction.
    return (Integer) JdbcTemplate.executeQuery(cm.getConnection(), COUNT_ENTRIES_SQL, new JdbcTemplate() {
      public Object results(ResultSet rs) throws SQLException {
        return rs.next() ? rs.getInt(1) : 0;
      }
    });
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public V put(final K key, final V value) {
    if (key == null || value == null) {
      throw new IllegalArgumentException("null is not supported.");
    }
    V prevValue = null;
    if (containsKey(key)) {
      Object[] idAndPrevValue = getIdAndValue(key);
      final long id = (Long) idAndPrevValue[0]; 
      prevValue = (V) idAndPrevValue[1];
      JdbcTemplate.executeUpdate(cm.getConnection(), UPDATE_ENTRY_SQL, new JdbcTemplate() {
        public void statement(PreparedStatement ps) throws SQLException {
          ps.setObject(1, value, Types.JAVA_OBJECT);
          ps.setObject(2, id, Types.BIGINT);
        }
      });
    }
    else {
      JdbcTemplate.executeUpdate(cm.getConnection(), INSERT_ENTRY_SQL, new JdbcTemplate() {
        public void statement(PreparedStatement ps) throws SQLException {
          ps.setObject(1, key.hashCode(), Types.INTEGER);
          ps.setObject(2, key, Types.JAVA_OBJECT);
          ps.setObject(3, value, Types.JAVA_OBJECT);
        }
      });
    }
    return prevValue;
  }
  
  @Override
  public boolean containsKey(final Object key) {
    return (Boolean) JdbcTemplate.executeQuery(cm.getConnection(), COUNT_ENTRIES_BY_KEY_HASHCODE_SQL,
      new JdbcTemplate() {
        public void statement(PreparedStatement ps) throws SQLException {
          ps.setObject(1, key.hashCode(), Types.INTEGER);
        }
        public Object results(ResultSet rs) throws SQLException {
          int count = 0;
          if (rs.next()) {
            count = rs.getInt(1);
          }
          return count > 0;
        }
      });
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(final Object key) {
    return (V) getIdAndValue(key)[1];
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  public Set<Map.Entry<K,V>> entrySet() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Returns the ID and value (in a two-element array) for the given key.
   */
  private Object[] getIdAndValue(final Object key) {
    return (Object[]) JdbcTemplate.executeQuery(cm.getConnection(), SELECT_ENTRY_BY_KEY_HASHCODE_SQL,
      new JdbcTemplate() {
        public void statement(PreparedStatement ps) throws SQLException {
          ps.setObject(1, key.hashCode(), Types.INTEGER);
        }
        public Object results(ResultSet rs) throws SQLException {
          long id = -1;
          Object value = null;
          if (rs.next()) {
            id = rs.getLong(1);
            value = rs.getObject(3);
            // If there is more than one result for this hashcode, keys must be compared the brute-force way
            // using Object.equals().
            if (rs.next()) {
              id = -1;
              value = null;
              rs.beforeFirst();
              while (rs.next() && value == null) {
                Object candidateKey = rs.getObject(2);
                if (key.equals(candidateKey)) {
                  id = rs.getLong(1);
                  value = rs.getObject(3);
                }
              }
            }
          }
          return new Object[] { id, value };
        }
      });
  }
}
