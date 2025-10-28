package com.minilog.log;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.zip.CRC32;
import com.minilog.log.Record;
import static java.nio.file.StandardOpenOption.*;

public class LogSegment {
    public static final int HEADER_BYTES = 12;
    public static final long MAX_SEGMENT_BYTES = 64L << 20;

    private final long baseOffset;
    private final Path filePath;

    private final FileChannel channel;
    private long nextRelativeOffset; // number of records so far
    private long sizeBytes;  

    public LogSegment(long baseOffset, Path dir) throws IOException {
        this.baseOffset = baseOffset;
        this.filePath = dir.resolve(Long.toString(baseOffset) + ".log");

        Files.createDirectories(dir);

        this.channel = FileChannel.open(filePath, CREATE, READ, WRITE);
        this.sizeBytes = this.channel.size();
        this.nextRelativeOffset = scanCount();
    }

    public synchronized long append(Record record) throws IOException {
        byte[] key = record.key();
        byte[] value = record.value();

        CRC32 crc = new CRC32();
        crc.update(key);
        crc.update(value);
        int checksum = (int) crc.getValue();

        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        header.putInt(checksum);
        header.putInt(key.length);
        header.putInt(value.length);
        header.flip();

        ByteBuffer payload = ByteBuffer.allocate(HEADER_BYTES + key.length + value.length);
        payload.putInt(checksum);
        payload.putInt(key.length);
        payload.putInt(value.length);
        payload.put(key);
        payload.put(value);
        payload.flip();

        channel.write(payload);
        channel.force(false);
        sizeBytes += HEADER_BYTES + key.length + value.length;
        long absoluteOffset = baseOffset + nextRelativeOffset;
        nextRelativeOffset++;
        return absoluteOffset;
        
    }

    public synchronized Record read(long targetOffset) throws IOException {
        long relativeOffset = targetOffset - baseOffset;
        if(relativeOffset < 0) throw new IllegalArgumentException("targetOffset before baseOffset");
        if(relativeOffset >= nextRelativeOffset) throw new IllegalArgumentException("Offset beyond end");
        long pos = 0;
        long size = channel.size();
        ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES);

        for(long i = 0; i < relativeOffset; i++){
            hdr.clear();
            channel.read(hdr, pos);
            hdr.flip();
            int crc = hdr.getInt();
            int klen = hdr.getInt();
            int vlen = hdr.getInt();
            long recv_size = HEADER_BYTES + klen + vlen;
            pos += recv_size;
        }

        hdr.clear();
        channel.read(hdr, pos);
        hdr.flip();
        int crc = hdr.getInt();
        int klen = hdr.getInt();
        int vlen = hdr.getInt();
        byte[] kv = new byte[klen + vlen];

        ByteBuffer buf = ByteBuffer.wrap(kv);
        channel.read(buf, pos + HEADER_BYTES);
        byte[] key = new byte[klen];
        byte[] value = new byte[vlen];
        System.arraycopy(kv, 0, key, 0, klen);
        System.arraycopy(kv, klen, value, 0, vlen);

        CRC32 check = new CRC32();
        check.update(key);
        check.update(value);
        if((int) check.getValue() != crc) {
            throw new IOException("CRC mismatch at offset "+ targetOffset);
        }
        return new Record(key, value);

    }
    public long nextOffset() {
        return baseOffset + nextRelativeOffset;
    }

    public boolean isFull(int recordBytes) {
        return sizeBytes + recordBytes + HEADER_BYTES > MAX_SEGMENT_BYTES;
    }

    private long scanCount() throws IOException {
        long count = 0;
        long pos = 0;
        long size = channel.size();
        ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES);

        while(pos + HEADER_BYTES <= size) {
            hdr.clear();
            channel.read(hdr, pos);
            hdr.flip();
            int crc = hdr.getInt();
            int klen = hdr.getInt();
            int vlen = hdr.getInt();
            long recSize = HEADER_BYTES + klen + vlen;
            if(pos + recSize > size) {
                break;
            }
            pos += recSize;
            count++;
        }
        return count;
    }

}
