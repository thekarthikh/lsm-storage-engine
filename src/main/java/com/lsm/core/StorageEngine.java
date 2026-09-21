package com.lsm.core;

import com.lsm.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Single-process LSM engine. Writers are serialized; readers share a read lock. */
public class StorageEngine implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);
    private final Path dataDir; private final long memTableThreshold; private final double bloomFilterFpr;
    private volatile MemTable activeMemTable; private final Deque<MemTable> immutableMemTables = new ArrayDeque<>();
    private final List<SSTable> ssTables = new ArrayList<>(); private WAL wal;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final CompactionManager compactionManager; private final ScheduledExecutorService compactionExecutor = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean closed; private long lastTimestamp;

    public StorageEngine(String dataDir, long memTableThreshold, double bloomFilterFpr) throws IOException {
        if (memTableThreshold <= 0) throw new IllegalArgumentException("memTableThreshold must be positive");
        this.dataDir = Paths.get(dataDir); this.memTableThreshold = memTableThreshold; this.bloomFilterFpr = bloomFilterFpr;
        Files.createDirectories(this.dataDir); this.activeMemTable = new MemTable(); this.compactionManager = new CompactionManager(this.dataDir, bloomFilterFpr);
        loadSSTables(); this.wal = new WAL(this.dataDir.resolve("wal.log")); recover(); startCompactionTask();
    }

    private void startCompactionTask() { compactionExecutor.scheduleWithFixedDelay(() -> { try { if (tableCount() >= 4) compactNow(); } catch (Exception e) { logger.error("Compaction failed", e); } }, 5, 5, TimeUnit.SECONDS); }
    private int tableCount() { lock.readLock().lock(); try { return ssTables.size(); } finally { lock.readLock().unlock(); } }

    public void put(byte[] key, byte[] value) throws IOException { write(value, false, key); }
    public void delete(byte[] key) throws IOException { write(null, true, key); }
    private void write(byte[] value, boolean tombstone, byte[] key) throws IOException {
        lock.writeLock().lock(); try { long timestamp = nextTimestamp(); writeRecord(tombstone ? Record.tombstone(key, timestamp) : new Record(key, value, timestamp)); } finally { lock.writeLock().unlock(); }
    }
    private void writeRecord(Record record) throws IOException { ensureOpen(); wal.append(record); activeMemTable.put(record); if (activeMemTable.getSizeInBytes() >= memTableThreshold) rotateMemTable(); }
    private long nextTimestamp() { long now = System.currentTimeMillis(); lastTimestamp = Math.max(now, lastTimestamp + 1); return lastTimestamp; }

    public byte[] get(byte[] key) throws IOException {
        lock.readLock().lock();
        try {
            Record record = activeMemTable.get(key); if (record != null) return visible(record);
            for (MemTable mt : immutableMemTables) { record = mt.get(key); if (record != null) return visible(record); }
            for (int i = ssTables.size() - 1; i >= 0; i--) { record = ssTables.get(i).get(key); if (record != null) return visible(record); }
            return null;
        } finally { lock.readLock().unlock(); }
    }
    private static byte[] visible(Record record) { return record.isTombstone() ? null : record.getValue(); }

    private void rotateMemTable() throws IOException {
        MemTable oldTable = activeMemTable; immutableMemTables.addFirst(oldTable); activeMemTable = new MemTable();
        Path oldWal = dataDir.resolve("wal.log"), archived = dataDir.resolve("wal_" + UUID.randomUUID() + ".log");
        wal.close(); move(oldWal, archived); wal = new WAL(oldWal);
        flushExecutor.submit(() -> { try { flush(oldTable, archived); } catch (IOException e) { logger.error("Flush failed", e); } });
    }
    private void flush(MemTable table, Path walPath) throws IOException {
        if (table.isEmpty()) { Files.deleteIfExists(walPath); removeImmutable(table); return; }
        Path path = dataDir.resolve("sstable_" + UUID.randomUUID() + ".db"); SSTable tableFile = new SSTable(path, table.getAll(), bloomFilterFpr);
        lock.writeLock().lock(); try { ssTables.add(tableFile); immutableMemTables.remove(table); } finally { lock.writeLock().unlock(); }
        Files.deleteIfExists(walPath); logger.info("Flushed {}", path.getFileName());
    }
    private void removeImmutable(MemTable table) { lock.writeLock().lock(); try { immutableMemTables.remove(table); } finally { lock.writeLock().unlock(); } }

    /** Test/maintenance hook: rotate and wait for all currently queued flushes. */
    public void flush() throws IOException { lock.writeLock().lock(); try { ensureOpen(); if (!activeMemTable.isEmpty()) rotateMemTable(); } finally { lock.writeLock().unlock(); } try { Future<?> barrier = flushExecutor.submit(() -> { }); barrier.get(30, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("flush interrupted", e); } catch (ExecutionException | TimeoutException e) { throw new IOException("flush timeout", e); } }

    public void compact() throws IOException {
        List<SSTable> candidates; lock.readLock().lock(); try { if (ssTables.size() < 2) return; candidates = new ArrayList<>(ssTables); } finally { lock.readLock().unlock(); }
        SSTable result = compactionManager.compact(candidates); if (result == null || result == candidates.get(0)) return;
        lock.writeLock().lock(); try {
            if (!ssTables.containsAll(candidates)) { Files.deleteIfExists(result.getPath()); return; }
            int insertionPoint = 0;
            for (int i = 0; i < ssTables.size(); i++) { if (candidates.contains(ssTables.get(i))) { insertionPoint = i; break; } }
            ssTables.removeAll(candidates); ssTables.add(Math.min(insertionPoint, ssTables.size()), result);
            for (SSTable old : candidates) Files.deleteIfExists(old.getPath());
        } finally { lock.writeLock().unlock(); }
    }
    private boolean compactNow() { try { compact(); return true; } catch (IOException e) { logger.error("Compaction failed", e); return false; } }

    private void recover() throws IOException {
        for (Record record : wal.replay()) { activeMemTable.put(record); lastTimestamp = Math.max(lastTimestamp, record.getTimestamp()); }
        File[] archived = dataDir.toFile().listFiles((dir, name) -> name.startsWith("wal_") && name.endsWith(".log"));
        if (archived == null) return;
        List<RecoveredWal> recoveredWals = new ArrayList<>();
        for (File file : archived) {
            WAL oldWal = new WAL(file.toPath()); List<Record> records; try { records = oldWal.replay(); } finally { oldWal.close(); }
            long maxTimestamp = records.stream().mapToLong(Record::getTimestamp).max().orElse(Long.MIN_VALUE);
            recoveredWals.add(new RecoveredWal(file.toPath(), records, maxTimestamp));
        }
        recoveredWals.sort(Comparator.comparingLong(walFile -> walFile.maxTimestamp));
        for (RecoveredWal walFile : recoveredWals) {
            MemTable recovered = new MemTable(); for (Record record : walFile.records) { recovered.put(record); lastTimestamp = Math.max(lastTimestamp, record.getTimestamp()); }
            if (!recovered.isEmpty()) flush(recovered, walFile.path); else Files.deleteIfExists(walFile.path);
        }
    }
    private void loadSSTables() throws IOException { File[] files = dataDir.toFile().listFiles((dir, name) -> name.endsWith(".db")); if (files == null) return; List<SSTable> loaded = new ArrayList<>(); for (File f : files) loaded.add(SSTable.load(f.toPath())); loaded.sort(Comparator.comparingLong(SSTable::getMaxTimestamp).thenComparingLong(SSTable::getCreatedAt).thenComparing(table -> table.getPath().getFileName().toString())); ssTables.addAll(loaded); }
    private static final class RecoveredWal { final Path path; final List<Record> records; final long maxTimestamp; RecoveredWal(Path path, List<Record> records, long maxTimestamp) { this.path = path; this.records = records; this.maxTimestamp = maxTimestamp; } }
    private static void move(Path from, Path to) throws IOException { try { Files.move(from, to, StandardCopyOption.ATOMIC_MOVE); } catch (AtomicMoveNotSupportedException e) { Files.move(from, to); } }
    private void ensureOpen() { if (closed) throw new IllegalStateException("storage engine is closed"); }
    @Override public void close() throws IOException { if (closed) return; closed = true; compactionExecutor.shutdown(); try { compactionExecutor.awaitTermination(30, TimeUnit.SECONDS); flushExecutor.shutdown(); flushExecutor.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } wal.close(); }
}
