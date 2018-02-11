package com.github.msteiger;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: describe
 */
public class Database implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private final MariaDbPoolDataSource pool;

    public Database(String jdbc) throws SQLException {
        // getting a connection from the pool just times out, so you don't know
        // what's wrong. So we run a test query first.
        MariaDbDataSource testSource = new MariaDbDataSource(jdbc);
        try (Connection conn = testSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
        }
        pool = new MariaDbPoolDataSource(jdbc);
    }

    public void insert(String table, Map<String, String> data) throws SQLException {
        char tq = '\u0060'; // MySQL and MariaDB use this char: `
        char fq = '\u0060'; // MySQL and MariaDB use this char: `
        char vq = '\'';
        String tableName = tq + table + tq;

        // there is no official guarantee that the iteration order is preserved
        String fieldList = joinQuoted(data.keySet(), fq);
        String valueList = joinQuoted(data.values(), vq);
        String sql = String.format("insert into %s (%s) values (%s);", tableName, fieldList, valueList);

        logger.trace("Executing SQL query: {}", sql);

        long time = System.nanoTime();
        try (Connection conn = pool.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery(sql);
            }
        }
        logger.debug("Took " + (System.nanoTime() - time) / 1000000 + " ms. to insert " + data.values());
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }

    private static String joinQuoted(Collection<?> list, char q) {
        StringBuffer sb = new StringBuffer();
        Iterator<?> it = list.iterator();
        while (it.hasNext()) {
            sb.append(q + String.valueOf(it.next()) + q);
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
