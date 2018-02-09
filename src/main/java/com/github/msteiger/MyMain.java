package com.github.msteiger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMain {

    private static final Logger logger = LoggerFactory.getLogger(MyMain.class);

    /**
     * @param argv ignored
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception {

        Path cfgFile = Paths.get("config.properties").toAbsolutePath();

        Properties props = readProperties(cfgFile);

        String host = props.getProperty("ebusd.host");
        int port = Integer.valueOf(props.getProperty("ebusd.port"));
        String message = props.getProperty("ebusd.message");
        String jdbcUrl = props.getProperty("database.jdbcUrl");

        try (Database db = new Database(jdbcUrl);
            EbusClient client = new EbusClient(host, port)) {
            List<String> fields = client.findFields(message);
            logger.debug("Fields: " + fields);
            client.startListening();

            while (!Thread.interrupted()) {
                record(message, fields, client, db);
            }
        }
    }

    private static void record(String message, List<String> fields, EbusClient client, Database db) throws SQLException {
        try {
            List<String> values = client.listen();
            Map<String, String> map = new LinkedHashMap<>();
            for (int i = 0; i < values.size(); i++) {
                map.put(fields.get(i), values.get(i));
            }
            db.insert(message, map);
        } catch (IOException e) {
            logger.error("Failed to process message", e);
        }
    }

    private static Properties readProperties(Path cfgFile) throws IOException {
        Properties props = new Properties();
        logger.info("Reading {}", cfgFile);
        try (BufferedReader reader = Files.newBufferedReader(cfgFile)) {
            props.load(reader);
        }
        return props;
    }
}
