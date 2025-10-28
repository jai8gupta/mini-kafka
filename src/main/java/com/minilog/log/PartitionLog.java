package com.minilog.log;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class PartitionLog {
    private final Path dir;
    private final List<LogSegment> segments = new ArrayList<>();

    public PartitionLog(Path dir) throws IOException{
        this.dir = dir;
        recover();
    }

    private void recover() throws IOException{
        Files.createDirectories(dir);
        var files = Files.list(dir).filter(p -> p.toString().endsWith(".log")).sorted(Comparator.comparingLong(PartitionLog::baseOffsetFromName)).toList();
        if(files.isEmpty()){
            segments.add(new LogSegment(0, dir));
            return;
        }

        for(Path p : files){
            long base = baseOffsetFromName(p);
            segments.add(new LogSegment(base, dir));
        }

    }

    public synchronized long append(Record record) throws IOException {
        LogSegment active = segments.get(segments.size()-1);
        int recvBytes = LogSegment.HEADER_BYTES + record.key().length + record.value().length;
        if (active.isFull(recvBytes)) {
            roll(active.nextOffset());
            active = segments.get(segments.size()-1);
        }
        return active.append(record);
    }

    public synchronized List<Record> fetch(long fromOffset, long maxRecords) throws IOException {
        List<Record> result = new ArrayList<>();

        for(LogSegment seg : segments){
            if(fromOffset < seg.nextOffset()){
                long curr = fromOffset;
                while (curr < seg.nextOffset() && result.size() < maxRecords) {
                    try {
                        Record rec = seg.read(curr);
                        result.add(rec);
                        curr++;
                    } catch(IllegalArgumentException e){
                        break;
                    }
                }
                fromOffset = seg.nextOffset();
                if(result.size() >= maxRecords) break;
            }
        }

        return result;
    }

    public void roll(long baseOffset) throws IOException {
        LogSegment newSegment = new LogSegment(baseOffset, dir);
        segments.add(newSegment);
    }


    private static long baseOffsetFromName(Path file){
        String name = file.getFileName().toString().replace(".log", "");
        return Long.parseLong(name);
    }
}
