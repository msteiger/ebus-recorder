package com.github.msteiger;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EbusClient implements Closeable {

    private final String host;
    private final int port;
    private SkipEmptyLineReader reader;
    private OutputStreamWriter writer;
    private Socket clientSocket;

    /**
     * @param host
     * @param port
     * @throws IOException
     */
    public EbusClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        reconnect();
    }

    public void reconnect() throws IOException {
        close();
        clientSocket = new Socket(host, port);
        writer = new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8);
        reader = new SkipEmptyLineReader(new BufferedReader(new InputStreamReader(clientSocket.getInputStream())));
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }

        if (reader != null) {
            reader.close();
            reader = null;
        }

        if (clientSocket != null) {
            clientSocket.close();
            clientSocket = null;
        }
    }

    public List<String> listen() throws IOException {
        String response = reader.readLine();
        String[] parts = response.split(" ", 4);
        if (parts.length != 4) {
            throw new IOException("Find response should be 4 parts, but is " + response);
        }

        String fields = parts[3];
        Pattern pattern = Pattern.compile("([^;]+)");
        Matcher m = pattern.matcher(fields);
        List<String> list = new ArrayList<>();
        while (m.find()) {
            list.add(m.group(1));
        }
        return list;
    }

    public void startListening() throws IOException {
        writer.write("listen\n");
        writer.flush();
        String response = reader.readLine();
        if (!response.equals("listen started") && !response.equals("listen continued")) {
            throw new IOException("could not start listening");
        }
    }

    public List<String> findFields(String message) throws IOException {
        writer.write("find -v " + message + "\n");
        writer.flush();

        String response = reader.readLine();

        // CIRCUIT   NAME  = FIELD[;FIELD]*
        // broadcast betrd = status=0;zustand=41;stellgrad=0;kesseltemp=43.0
        String[] parts = response.split(" ", 4);
        if (parts.length != 4) {
            throw new IOException("Find response should be 4 parts, but is " + response);
        }

        String fields = parts[3];
        Pattern pattern = Pattern.compile("(\\w+)=[^;]+");
        Matcher m = pattern.matcher(fields);
        List<String> list = new ArrayList<>();
        while (m.find()) {
            list.add(m.group(1));
        }
        return list;
    }
}
