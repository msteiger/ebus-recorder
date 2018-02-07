package com.github.msteiger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyMain {

    public static void main(String argv[]) throws Exception {

        String host = "raspberrypi";
        int port = 8888;

        String message = "betrd";

        EbusClient client = new EbusClient(host, port);
        client.connect();
        List<String> fields = client.findFields(message);
        System.out.println("Fields: " + fields);
        client.startListening();
        boolean run = true;
        while (run) {
            try {
                List<String> values = client.listen();
                Map<String, String> map = new HashMap<>();
                for (int i = 0; i < values.size(); i++) {
                    map.put(fields.get(i), values.get(i));
                }
            } catch (IOException e) {

            }
            run = false;
        }
        client.close();
    }
}
