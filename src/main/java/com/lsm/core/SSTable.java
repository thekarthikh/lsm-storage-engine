package com.lsm.core;

import com.lsm.model.Record;
import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.*;

public class SSTable {
    private final Path path;
    private final BloomFilter bloomFilter;
    private final Map<byte[], Long> index;
    private final long createdAt;

    public SSTable(Path path, ConcurrentNavigableMap<byte[], Record> data, double fpr) throws IOException {
        this.path = path;
        this.createdAt = System.currentTimeMillis();
        this.bloomFilter = new BloomFilter(data.size(), fpr);
        this.index = new TreeMap<>(Arrays::compare);

        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             DataOutputStream dos = new DataOutputStream(bos)) {
            
            long offset = 0;
            for (Record record : data.values()) {
                bloomFilter.add(record.getKey());
                index.put(record.getKey(), offset);
                
                byte[] key = record.getKey();
                byte[] value = record.isTombstone() ? new byte[0] : record.getValue();
                
                dos.writeLong(record.getTimestamp());
                dos.writeBoolean(record.isTombstone());
                dos.writeInt(key.length);
                dos.write(key);
                dos.writeInt(value.length);
                dos.write(value);
                
                offset += 8 + 1 + 4 + key.length + 4 + value.length;
            }
            
            // Write Index
            long indexOffset = offset;
            dos.writeInt(index.size());
            for (Map.Entry<byte[], Long> entry : index.entrySet()) {
                dos.writeInt(entry.getKey().length);
                dos.write(entry.getKey());
                dos.writeLong(entry.getValue());
            }
            
            // Write Bloom Filter
            long filterOffset = dos.size(); // Approximate
            bloomFilter.serialize(dos);
            
            // Write Footer
            dos.writeLong(indexOffset);
            dos.writeLong(filterOffset);
        }
    }

    private SSTable(Path path, BloomFilter filter, Map<byte[], Long> index, long createdAt) {
        this.path = path;
        this.bloomFilter = filter;
        this.index = index;
        this.createdAt = createdAt;
    }

    public static SSTable load(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long length = raf.length();
            raf.seek(length - 16);
            long indexOffset = raf.readLong();
            long filterOffset = raf.readLong();
            
            // Load Index
            raf.seek(indexOffset);
            int indexSize = raf.readInt();
            Map<byte[], Long> index = new TreeMap<>(Arrays::compare);
            for (int i = 0; i < indexSize; i++) {
                int keyLen = raf.readInt();
                byte[] key = new byte[keyLen];
                raf.readFully(key);
                long offset = raf.readLong();
                index.put(key, offset);
            }
            
            // Load Bloom Filter
            raf.seek(filterOffset);
            byte[] filterBytes = new byte[(int)(length - 16 - filterOffset)];
            raf.readFully(filterBytes);
            BloomFilter filter = BloomFilter.deserialize(new ByteArrayInputStream(filterBytes));
            
            return new SSTable(path, filter, index, path.toFile().lastModified());
        }
    }

    public Record get(byte[] key) throws IOException {
        if (!bloomFilter.mightContain(key)) {
            return null;
        }
        
        Long offset = index.get(key);
        if (offset == null) return null;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(offset);
            long timestamp = raf.readLong();
            boolean tombstone = raf.readBoolean();
            int keyLen = raf.readInt();
            byte[] recordedKey = new byte[keyLen];
            raf.readFully(recordedKey);
            int valLen = raf.readInt();
            byte[] value = new byte[valLen];
            raf.readFully(value);
            
            if (Arrays.equals(key, recordedKey)) {
                return new Record(recordedKey, tombstone ? null : value, timestamp, tombstone);
            }
        }
        return null;
    }

    public Iterable<Record> scan() {
        return () -> new Iterator<Record>() {
            private final Iterator<Long> offsetIterator = index.values().iterator();
            private RandomAccessFile raf;

            {
                try {
                    raf = new RandomAccessFile(path.toFile(), "r");
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                boolean has = offsetIterator.hasNext();
                if (!has) {
                    try { raf.close(); } catch (IOException ignored) {}
                }
                return has;
            }

            @Override
            public Record next() {
                try {
                    raf.seek(offsetIterator.next());
                    long timestamp = raf.readLong();
                    boolean tombstone = raf.readBoolean();
                    int keyLen = raf.readInt();
                    byte[] key = new byte[keyLen];
                    raf.readFully(key);
                    int valLen = raf.readInt();
                    byte[] value = new byte[valLen];
                    raf.readFully(value);
                    return new Record(key, tombstone ? null : value, timestamp, tombstone);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public Path getPath() {
        return path;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
