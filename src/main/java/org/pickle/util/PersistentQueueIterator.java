/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.util;

import java.util.*;
import java.sql.*;
import org.apache.log4j.Logger;
import org.pickle.sql.*;

class PersistentQueueIterator<E> implements Iterator<E> {

    private static final Logger log = Logger.getLogger(PersistentQueueIterator.class);

    private final ConnectionManager cm;
    private Long currentId;

    public PersistentQueueIterator(ConnectionManager cm) {
        this.cm = cm;
        currentId = -1L;
    }

    @Override
    public boolean hasNext() {
        return nextId() != null;
    }

    @Override
    public E next() {
        log.debug("Current ID = " + currentId);
        currentId = nextId();
        log.debug("Next ID = " + currentId);
        if (currentId != null) {
            @SuppressWarnings("unchecked")
            E object = (E) JdbcTemplate.executeQuery(cm.getConnection(), PersistentQueue.SQL.SELECT_OBJECT,
                    new JdbcTemplate() {
                        @Override
                        public void statement(PreparedStatement ps) throws SQLException {
                            ps.setLong(1, currentId);
                        }

                        @Override
                        public Object results(ResultSet rs) throws SQLException {
                            return rs.next() ? rs.getObject(1) : null;
                        }
                    });
            log.debug("Returning object with ID = " + currentId);
            return object;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        if (currentId != null) {
            log.debug("Removing ID = " + currentId);
            JdbcTemplate.executeUpdate(cm.getConnection(), PersistentQueue.SQL.DELETE_OBJECT, new JdbcTemplate() {
                @Override
                public void statement(PreparedStatement ps) throws SQLException {
                    ps.setLong(1, currentId);
                }
            });
        } else {
            throw new NoSuchElementException();
        }
    }

    private Long nextId() {
        if (currentId != null) {
            return (Long) JdbcTemplate.executeQuery(cm.getConnection(), PersistentQueue.SQL.SELECT_NEXT_ID,
                    new JdbcTemplate() {
                        @Override
                        public void statement(PreparedStatement ps) throws SQLException {
                            ps.setLong(1, currentId);
                        }

                        @Override
                        public Object results(ResultSet rs) throws SQLException {
                            Long id = null;
                            if (rs.next()) {
                                // According to ResultSet#getLong(...) Javadocs, SQL NULL is returned as 0.
                                id = rs.getLong(1);
                                if (id == 0) {
                                    id = null;
                                }
                            }
                            return id;
                        }
                    });
        } else {
            return null;
        }
    }
}
