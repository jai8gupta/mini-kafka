package com.minilog.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;


public class PartitionManager {
    private final Path path;
    private final ConcurrentHashMap<String, PartitionLog> partion = new ConcurrentHashMap<>();

    public PartitionManager(Path dir){
        this.path = dir;
    }

    public PartitionLog get(String topic) throws IOException {
        return partion.computeIfAbsent(topic, t -> {
            try {
                return new PartitionLog(path.resolve(t));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
}
