/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.sql;

import java.sql.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * JdbcTemplate eases the use of JDBC by providing standard strategies for
 * executing SQL statements and queries. The static methods
 * {@link #executeUpdate} and {@link #executeQuery} handle the actual execution
 * and cleanup required by the JDBC classes. They accept a custom JdbcTemplate
 * subclass whose methods are called by the static methods at appropriate points
 * in the exection process.
 *
 * Typically the JdbcTemplate is implemented as an anonymous inner class by the
 * caller:
 * <pre><code>
 * // Count all the Freds.
 * Connection c = ...
 * final String firstName = "Fred";
 * int count = (Integer) JdbcTemplate.executeQuery(
 *   c, "select count(*) from user where firstname = ?",
 *   new JdbcTemplate() {
 *     public void statement(PreparedStatement ps) throws SQLException {
 *       ps.setString(1, firstName);
 *     }
 *     public Object results(ResultSet rs) throws SQLException {
 *       return rs.next() ? rs.getInt(1) : 0;
 *     }
 *   });
 * </code></pre>
 */
public class JdbcTemplate {
    private static final Logger log = Logger.getLogger(JdbcTemplate.class);

    private String sql;

    /**
     * Called by JdbcTemplate.executeUpdate() and JdbcTemplate.executeQuery()
     * immediately after the PreparedStatement is created. This method should
     * assign placeholder values on the statement.
     */
    public void statement(PreparedStatement ps) throws SQLException {
    }

    /**
     * Called by JdbcTemplate.executeQuery() with the return value of
     * PreparedStatement.executeQuery(). This method's return value will be
     * returned by JdbcTemplate.executeQuery(). The input ResultSet should not
     * be returned since it will be closed by the JdbcTemplate.executeQuery().
     */
    public Object results(ResultSet rs) throws SQLException {
        return null;
    }

    /**
     * Called by JdbcTemplate.executeUpdate() with the return value of
     * PreparedStatement.executeUpdate(), which is the number of rows affected
     * by the insert, update, or delete statement. This call occurs before the
     * transaction is committed. If this method throws an exception, the
     * transaction is rolled back.
     *
     * @param count the number of affected rows
     */
    public void updated(int count) {
    }

    /**
     * Returns the SQL string currently being executed.
     */
    protected final String getSql() {
        return sql;
    }

    /**
     * Executes a SQL statement with the default JdbcTemplate instance.
     */
    public static void executeUpdate(Connection connection, String sql) {
        executeUpdate(connection, sql, new JdbcTemplate());
    }

    /**
     * Executes a SQL statement using a JdbcTemplate.
     */
    public static void executeUpdate(Connection connection, String sql, JdbcTemplate template) {
        template.sql = sql;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            template.statement(statement);
            template.updated(statement.executeUpdate());
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new DatabaseException("Unable to execute SQL statement: " + sql, e);
        } catch (RuntimeException e) {
            rollback(connection);
            throw e;
        } finally {
            close(null, statement);
        }
    }

    /**
     * Executes a SQL query using a JdbcTemplate.
     */
    public static Object executeQuery(Connection connection, String sql, JdbcTemplate template) {
        template.sql = sql;
        PreparedStatement statement = null;
        ResultSet results = null;
        try {
            statement = connection.prepareStatement(sql);
            template.statement(statement);
            results = statement.executeQuery();
            return template.results(results);
        } catch (SQLException e) {
            throw new DatabaseException("Unable to execute SQL query: " + sql, e);
        } finally {
            close(results, statement);
        }
    }

    public static void openTransaction(ConnectionManager cm) {
        // TODO: Store connection in thread local
    }

    public static void commitTransaction() {
        // TODO: Commit
    }

    public static void rollbackTransaction() {
        // TODO: Rollback
    }

    private static void close(ResultSet results, Statement statement) {
        if (results != null) {
            try {
                results.close();
            } catch (SQLException e) {
                log.log(Level.WARN,"A ResultSet could not be closed.", e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.warn("A Statement could not be closed.", e);
            }
        }
    }

    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            log.warn("A Connection transaction could not be rolled back.", e);
        }
    }
}
