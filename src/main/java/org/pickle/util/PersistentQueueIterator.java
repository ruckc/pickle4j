/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.util.*;
import java.sql.*;
import org.pickle.sql.*;
import org.pickle.logging.Log;

class PersistentQueueIterator<E> implements Iterator<E> {

  private ConnectionManager cm;
  private Long currentId;
  
  public PersistentQueueIterator(ConnectionManager cm) {
    this.cm = cm;
    currentId = -1L;
  }
  
  public boolean hasNext() {
    return nextId() != null;
  }
  
  public E next() {
    Log.debug("Current ID = %d", currentId);
    currentId = nextId();
    Log.debug("Next ID = %d", currentId);
    if (currentId != null) {
      @SuppressWarnings("unchecked")
      E object = (E) JdbcTemplate.executeQuery(cm.getConnection(), PersistentQueue.SQL.SELECT_OBJECT,
        new JdbcTemplate() {
          public void statement(PreparedStatement ps) throws SQLException {
            ps.setLong(1, currentId);
          }
          public Object results(ResultSet rs) throws SQLException {
            return rs.next() ? rs.getObject(1) : null;
          }
        });
      Log.debug("Returning object with ID = %d", currentId);
      return object;
    }
    else {
      throw new NoSuchElementException();
    }
  }
  
  public void remove() {
    if (currentId != null) {
      Log.debug("Removing ID = %d", currentId);
      JdbcTemplate.executeUpdate(cm.getConnection(), PersistentQueue.SQL.DELETE_OBJECT, new JdbcTemplate() {
        public void statement(PreparedStatement ps) throws SQLException {
          ps.setLong(1, currentId);
        }
      });
    }
    else {
      throw new NoSuchElementException();
    }
  }

  private Long nextId() {
    if (currentId != null) {
      return (Long) JdbcTemplate.executeQuery(cm.getConnection(), PersistentQueue.SQL.SELECT_NEXT_ID,
        new JdbcTemplate() {
          public void statement(PreparedStatement ps) throws SQLException {
            ps.setLong(1, currentId);
          }
          public Object results(ResultSet rs) throws SQLException {
            Long id = null;
            if (rs.next()) {
              // According to ResultSet#getLong(...) Javadocs, SQL NULL is returned as 0.
              id = rs.getLong(1);
              if (id == 0) id = null;
            }
            return id;
          }
        });
    }
    else {
      return null;
    }
  }
}
