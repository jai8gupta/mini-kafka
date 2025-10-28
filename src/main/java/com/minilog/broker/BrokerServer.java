package com.minilog.broker;

import com.minilog.log.PartitionManager;
import java.net.*;
import java.nio.file.Path;

public class BrokerServer {
    public static void main() throws Exception{
        int port = 9092;
        var dataDir = Path.of("data");
        var partitions = new PartitionManager(dataDir);
        try (ServerSocket socket = new ServerSocket(port)) {
            System.out.println("MiniKafka Broker listening on port " + port);
            while (true) {
                Socket client = socket.accept();
                System.out.println("Client connected: " + client);
                new Thread(new BrokerHandler(client, partitions)).start();
            }
            
        }
    }
}
