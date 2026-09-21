package com.lsm.core;

import com.lsm.model.Record;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/** Immutable SSTable. The footer is written last and is the publication marker. */
public class SSTable {
    private static final int MAGIC = 0x4C534D31;
    private static final int VERSION = 1;
    private static final int FOOTER_BYTES = 4 + 4 + 8 + 8 + 8 + 8;
    private static final int MAX_KEY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_VALUE_BYTES = 256 * 1024 * 1024;
    private final Path path;
    private final BloomFilter bloomFilter;
    private final NavigableMap<byte[], Long> index;
    private final long createdAt;
    private long maxTimestamp;

    public SSTable(Path path, ConcurrentNavigableMap<byte[], Record> data, double fpr) throws IOException {
        this.path = path; this.createdAt = System.currentTimeMillis(); this.maxTimestamp = Long.MIN_VALUE;
        long highestTimestamp = Long.MIN_VALUE;
        this.bloomFilter = new BloomFilter(Math.max(1, data.size()), fpr);
        this.index = new TreeMap<>(Arrays::compare);
        Path temp = path.resolveSibling(path.getFileName() + ".tmp"); Files.deleteIfExists(temp);
        try (FileOutputStream fos = new FileOutputStream(temp.toFile()); BufferedOutputStream bos = new BufferedOutputStream(fos); DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(MAGIC); dos.writeInt(VERSION); long offset = 8;
            for (Record record : data.values()) {
                if (record == null || record.getKey() == null) throw new IllegalArgumentException("invalid record");
                byte[] key = record.getKey(); byte[] value = record.isTombstone() ? new byte[0] : record.getValue();
                highestTimestamp = Math.max(highestTimestamp, record.getTimestamp());
                bloomFilter.add(key); index.put(key, offset);
                dos.writeLong(record.getTimestamp()); dos.writeBoolean(record.isTombstone()); dos.writeInt(key.length); dos.write(key); dos.writeInt(value.length); dos.write(value);
                offset += 8 + 1 + 4 + key.length + 4 + value.length;
            }
            this.maxTimestamp = highestTimestamp;
            long indexOffset = offset; dos.writeInt(index.size());
            for (Map.Entry<byte[], Long> e : index.entrySet()) { dos.writeInt(e.getKey().length); dos.write(e.getKey()); dos.writeLong(e.getValue()); }
            long filterOffset = indexOffset + 4 + index.entrySet().stream().mapToLong(e -> 4L + e.getKey().length + 8L).sum();
            bloomFilter.serialize(dos); dos.writeInt(MAGIC); dos.writeInt(VERSION); dos.writeLong(indexOffset); dos.writeLong(filterOffset); dos.writeLong(createdAt); dos.writeLong(highestTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : highestTimestamp);
            dos.flush(); fos.getFD().sync();
        }
        moveIntoPlace(temp, path);
    }

    private SSTable(Path path, BloomFilter filter, NavigableMap<byte[], Long> index, long createdAt, long maxTimestamp) { this.path = path; this.bloomFilter = filter; this.index = index; this.createdAt = createdAt; this.maxTimestamp = maxTimestamp; }

    public static SSTable load(Path path) throws IOException {
        if (!Files.isRegularFile(path)) throw new IOException("SSTable is not a regular file: " + path);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long length = raf.length(); if (length < 8 + FOOTER_BYTES) throw new IOException("truncated SSTable: " + path);
            raf.seek(length - FOOTER_BYTES);
            if (raf.readInt() != MAGIC || raf.readInt() != VERSION) throw new IOException("invalid SSTable footer: " + path);
            long indexOffset = raf.readLong(), filterOffset = raf.readLong(), createdAt = raf.readLong(), maxTimestamp = raf.readLong();
            if (indexOffset < 8 || indexOffset >= filterOffset || filterOffset >= length - FOOTER_BYTES) throw new IOException("invalid SSTable offsets: " + path);
            raf.seek(0); if (raf.readInt() != MAGIC || raf.readInt() != VERSION) throw new IOException("invalid SSTable header: " + path);
            raf.seek(indexOffset); int count = raf.readInt(); if (count < 0) throw new IOException("invalid SSTable index size");
            NavigableMap<byte[], Long> index = new TreeMap<>(Arrays::compare); long previous = -1;
            byte[] previousKey = null;
            for (int i = 0; i < count; i++) { int keyLen = raf.readInt(); if (keyLen < 0 || keyLen > MAX_KEY_BYTES) throw new IOException("invalid SSTable key length"); byte[] key = new byte[keyLen]; raf.readFully(key); long offset = raf.readLong(); if (offset < 8 || offset >= indexOffset || offset <= previous || (previousKey != null && Arrays.compare(previousKey, key) >= 0)) throw new IOException("invalid SSTable index ordering"); previous = offset; previousKey = key; index.put(key, offset); }
            if (raf.getFilePointer() != filterOffset) throw new IOException("SSTable index/filter boundary mismatch");
            raf.seek(filterOffset); byte[] filterBytes = new byte[(int)(length - FOOTER_BYTES - filterOffset)]; raf.readFully(filterBytes);
            return new SSTable(path, BloomFilter.deserialize(new ByteArrayInputStream(filterBytes)), index, createdAt, maxTimestamp);
        } catch (EOFException | NegativeArraySizeException e) { throw new IOException("truncated or malformed SSTable: " + path, e); }
    }

    public Record get(byte[] key) throws IOException {
        if (!bloomFilter.mightContain(key)) return null; Long offset = index.get(key); if (offset == null) return null;
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) { Record record = readRecord(raf, offset, indexOffset()); return Arrays.equals(key, record.getKey()) ? record : null; }
    }

    public Iterable<Record> scan() throws IOException {
        List<Record> records = new ArrayList<>(index.size());
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) { long limit = indexOffset(); for (long offset : index.values()) records.add(readRecord(raf, offset, limit)); }
        return records;
    }

    private long indexOffset() throws IOException { try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) { raf.seek(raf.length() - FOOTER_BYTES); raf.readInt(); raf.readInt(); return raf.readLong(); } }
    private static Record readRecord(RandomAccessFile raf, long offset, long limit) throws IOException { raf.seek(offset); long timestamp = raf.readLong(); boolean tombstone = raf.readBoolean(); int keyLen = raf.readInt(); if (keyLen < 0 || keyLen > MAX_KEY_BYTES) throw new IOException("invalid SSTable key length"); byte[] key = new byte[keyLen]; raf.readFully(key); int valueLen = raf.readInt(); if (valueLen < 0 || valueLen > MAX_VALUE_BYTES || raf.getFilePointer() + valueLen > limit || (tombstone && valueLen != 0)) throw new IOException("invalid SSTable value length"); byte[] value = new byte[valueLen]; raf.readFully(value); return new Record(key, tombstone ? null : value, timestamp, tombstone); }
    private static void moveIntoPlace(Path temp, Path path) throws IOException { try { Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); } catch (AtomicMoveNotSupportedException e) { Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING); } }
    public Path getPath() { return path; }
    public long getCreatedAt() { return createdAt; }
    public long getMaxTimestamp() { return maxTimestamp; }
}
