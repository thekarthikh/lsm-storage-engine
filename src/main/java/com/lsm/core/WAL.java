package com.lsm.core;

import com.lsm.model.Record;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WAL implements AutoCloseable {
    private static final int MAX_KEY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_VALUE_BYTES = 256 * 1024 * 1024;
    private static final int MIN_FRAME_BYTES = 8 + 1 + 4 + 4;
    private static final int MAX_FRAME_BYTES = MIN_FRAME_BYTES + MAX_KEY_BYTES + MAX_VALUE_BYTES;
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
        if (key == null || key.length > MAX_KEY_BYTES || (!record.isTombstone() && (value == null || value.length > MAX_VALUE_BYTES))) {
            throw new IllegalArgumentException("record exceeds WAL limits");
        }
        int valueLen = record.isTombstone() ? -1 : value.length;
        int frameLength = MIN_FRAME_BYTES + key.length + (valueLen < 0 ? 0 : valueLen);
        ByteBuffer buffer = ByteBuffer.allocate(4 + frameLength);
        buffer.putInt(frameLength);
        buffer.putLong(record.getTimestamp());
        buffer.put((byte) (record.isTombstone() ? 1 : 0));
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

    public synchronized List<Record> replay() throws IOException {
        List<Record> records = new ArrayList<>();
        if (!path.toFile().exists() || path.toFile().length() == 0) return records;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            while (true) {
                long frameStart = raf.getFilePointer();
                final int frameLength;
                try { frameLength = raf.readInt(); } catch (EOFException end) { channel.truncate(frameStart); break; }
                if (frameLength < MIN_FRAME_BYTES || frameLength > MAX_FRAME_BYTES) {
                    throw new IOException("invalid WAL frame length: " + frameLength);
                }
                byte[] frame = new byte[frameLength];
                try { raf.readFully(frame); } catch (EOFException truncatedTail) { channel.truncate(frameStart); break; }
                DataInputStream recordIn = new DataInputStream(new ByteArrayInputStream(frame));
                long timestamp = recordIn.readLong();
                int flags = recordIn.readUnsignedByte();
                int keyLen = recordIn.readInt();
                if (keyLen < 0 || keyLen > MAX_KEY_BYTES || keyLen > recordIn.available()) throw new IOException("invalid WAL key length");
                byte[] key = new byte[keyLen]; recordIn.readFully(key);
                int valueLen = recordIn.readInt();
                if (flags == 1 && valueLen != -1) throw new IOException("invalid tombstone WAL record");
                if (flags != 0 && flags != 1) throw new IOException("invalid WAL flags");
                if (valueLen < -1 || valueLen > MAX_VALUE_BYTES || valueLen > recordIn.available()) throw new IOException("invalid WAL value length");
                if (valueLen == -1) records.add(Record.tombstone(key, timestamp));
                else { byte[] value = new byte[valueLen]; recordIn.readFully(value); records.add(new Record(key, value, timestamp)); }
                if (recordIn.available() != 0) throw new IOException("trailing bytes in WAL record");
            }
        }
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
