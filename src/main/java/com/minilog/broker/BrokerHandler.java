package com.minilog.broker;

import com.minilog.log.*;
import java.io.*;
import java.net.Socket;
import java.util.List;
import com.minilog.log.Record;

public class BrokerHandler implements Runnable {
    private final Socket socket;
    private final PartitionManager partitionManager;

    public BrokerHandler(Socket socket, PartitionManager partitionManager) {
        this.socket = socket;
        this.partitionManager = partitionManager;
    }

    @Override
    public void run() {
        try (
            socket;
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("Received: " + line);
                String[] parts = line.split(" ");
                if (parts.length == 0) continue;

                String cmd = parts[0].toUpperCase();

                switch (cmd) {
                    case "PRODUCE" -> {
                        if (parts.length < 4) {
                            out.write("ERR Usage: PRODUCE <topic> <key> <value>\n");
                        } else {
                            String topic = parts[1];
                            byte[] key = parts[2].getBytes();
                            byte[] value = parts[3].getBytes();

                            long offset = partitionManager.get(topic)
                                    .append(new Record(key, value));
                            out.write("OK offset=" + offset + "\n");
                        }
                        out.flush();
                    }

                    case "FETCH" -> {
                        if (parts.length < 4) {
                            out.write("ERR Usage: FETCH <topic> <offset> <count>\n");
                        } else {
                            String topic = parts[1];
                            long from = Long.parseLong(parts[2]);
                            int count = Integer.parseInt(parts[3]);
                            List<Record> recs = partitionManager.get(topic).fetch(from, count);
                            for (Record r : recs) {
                                out.write(new String(r.key()) + "=" + new String(r.value()) + "\n");
                            }
                            out.write("END\n");
                        }
                        out.flush();
                    }

                    case "QUIT" -> {
                        out.write("BYE\n");
                        out.flush();
                        return;
                    }

                    default -> {
                        out.write("ERR Unknown command\n");
                        out.flush();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }
}
