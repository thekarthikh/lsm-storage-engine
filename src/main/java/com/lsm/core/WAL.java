package com.lsm.core;

import com.lsm.model.Record;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WAL {
    private final FileChannel channel;
    private final Path path;

    public WAL(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.APPEND, 
            StandardOpenOption.WRITE);
    }

    public synchronized void append(Record record) throws IOException {
        byte[] key = record.getKey();
        byte[] value = record.getValue();
        int valueLen = record.isTombstone() ? -1 : value.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + key.length + 4 + (record.isTombstone() ? 0 : value.length));
        buffer.putLong(record.getTimestamp());
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(valueLen);
        if (!record.isTombstone()) {
            buffer.put(value);
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(true); // fsync
    }

    public List<Record> replay() throws IOException {
        List<Record> records = new ArrayList<>();
        if (!path.toFile().exists() || path.toFile().length() == 0) return records;

        try (FileInputStream fis = new FileInputStream(path.toFile());
             DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {
            while (dis.available() > 0) {
                long timestamp = dis.readLong();
                int keyLen = dis.readInt();
                byte[] key = new byte[keyLen];
                dis.readFully(key);
                int valueLen = dis.readInt();
                if (valueLen == -1) {
                    records.add(Record.tombstone(key, timestamp));
                } else {
                    byte[] value = new byte[valueLen];
                    dis.readFully(value);
                    records.add(new Record(key, value, timestamp));
                }
            }
        } catch (EOFException ignored) {}
        return records;
    }

    public void close() throws IOException {
        channel.close();
    }

    public void delete() throws IOException {
        close();
        path.toFile().delete();
    }
}
