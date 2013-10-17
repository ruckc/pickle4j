/*
 * Copyright (c) 2008, Steven R. Farley.  Licensed under a BSD-like license (see LICENSE.TXT).
 */
package org.pickle.sql;

import java.io.*;
import java.sql.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;
import org.h2.tools.Script;

import org.pickle.Disposable;

/**
 * This class creates or opens an H2 database and provides access to a JDBC
 * Connection to the database. The connection is automatically closed on
 * finalize(), but calling dispose() is preferable.
 */
public class ConnectionManager implements Disposable {
    private static final Logger log = Logger.getLogger(ConnectionManager.class);
    private final String dbName = "pickle";
    private final File dataDir;
    private final String createSql;
    private final String jdbcUrl;
    private Connection connection;

    /**
     * Opens an H2 database located in dataDir. If the database does not exist,
     * it is created. The createSql script is executed each time, so it must
     * insure SQL uses statements such as "IF NOT EXISTS" where appropriate.
     *
     * @param dataDir the base directory of the database
     * @param createSql the SQL creation script
     */
    public ConnectionManager(File dataDir, String createSql) {
        this.dataDir = dataDir;
        this.createSql = createSql;
        this.jdbcUrl = String.format("jdbc:h2:%s/%s", dataDir.getAbsolutePath(), dbName);
        init();

        JdbcTemplate.executeUpdate(connection, createSql);
    }

    private void init() {
        if (!dataDir.exists() || !dataDir.isDirectory() || !dataDir.canWrite()) {
            throw new DatabaseException(
                    String.format("'%s' is not a writable directory or does not exist.", dataDir));
        } else {
            try {
                JdbcDataSource ds = new JdbcDataSource();
                ds.setURL(jdbcUrl);
                ds.setUser("sa");
                ds.setPassword("");
                connection = ds.getConnection();
                log.info("Opened database connection to "+jdbcUrl);
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                throw new DatabaseException("Unable to connect to H2 database at: " + jdbcUrl, e);
            }
        }
    }

    public void compact() {
        try {
            long start = System.currentTimeMillis();
            dispose();
            File tempFile = File.createTempFile(dataDir.getName(),"-pickle.sql.gz");
            long initialSize = getDirectorySize(dataDir);
            try (FileOutputStream file = new FileOutputStream(tempFile)) {
                GZIPOutputStream out = new GZIPOutputStream(file);
                Script.execute(jdbcUrl, "sa", "", out);
                out.close();
            }
            DeleteDbFiles.execute(dataDir.getAbsolutePath(), dbName, true);
            init();
            FileInputStream fileIn = new FileInputStream(tempFile);
            GZIPInputStream gzipIn = new GZIPInputStream(fileIn);
            InputStreamReader in = new InputStreamReader(gzipIn);
            RunScript.execute(connection, in);
            tempFile.delete();
            long stop = System.currentTimeMillis();
            log.info("Reclaimed space: "+(initialSize-getDirectorySize(dataDir))+" in "+(stop-start)+"ms");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(),e);
        }
    }
    
    public long getDatabaseSize() {
        return getDirectorySize(dataDir);
    }
    
    private long getDirectorySize(File dir) {
        long size = 0;
        for(File f : dir.listFiles()) {
            if(f.isDirectory()) {
                size += getDirectorySize(f);
            } else if(f.isFile()) {
                size += f.length();
            }
        }
        return size;
    }

    /**
     * Returns a JDBC Connection to the H2 database.
     *
     * @return the Connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Closes the JDBC Connection. This ConnectionManager becomes unusable.
     */
    @Override
    public void dispose() {
        try {
            if (!connection.isClosed()) {
                connection.close();
                log.info("Closed database connection to "+jdbcUrl);
            }
        } catch (SQLException e) {
            log.warn("Failed to close database connection to "+jdbcUrl);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (connection != null) {
            try {
                dispose();
            } finally {
                super.finalize();
            }
        }
    }
}