package com.lsm.core;

import com.lsm.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);
    
    private final Path dataDir;
    private final long memTableThreshold;
    private final double bloomFilterFpr;
    
    private volatile MemTable activeMemTable;
    private final Deque<MemTable> immutableMemTables = new ConcurrentLinkedDeque<>();
    private final List<SSTable> ssTables = new CopyOnWriteArrayList<>();
    
    private WAL wal;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    
    private final CompactionManager compactionManager;
    private final ScheduledExecutorService compactionExecutor = Executors.newSingleThreadScheduledExecutor();

    public StorageEngine(String dataDir, long memTableThreshold, double bloomFilterFpr) throws IOException {
        this.dataDir = Paths.get(dataDir);
        this.memTableThreshold = memTableThreshold;
        this.bloomFilterFpr = bloomFilterFpr;
        
        if (!Files.exists(this.dataDir)) {
            Files.createDirectories(this.dataDir);
        }
        
        this.activeMemTable = new MemTable();
        this.wal = new WAL(this.dataDir.resolve("wal.log"));
        this.compactionManager = new CompactionManager(this.dataDir, bloomFilterFpr);
        
        recover();
        loadSSTables();
        startCompactionTask();
    }

    private void startCompactionTask() {
        compactionExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (ssTables.size() >= 4) { // Simple size-tiered trigger
                    List<SSTable> toCompact = new ArrayList<>(ssTables);
                    SSTable result = compactionManager.compact(toCompact);
                    if (result != null) {
                        lock.writeLock().lock();
                        try {
                            ssTables.removeAll(toCompact);
                            ssTables.add(result);
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Compaction failed", e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        lock.writeLock().lock();
        try {
            Record record = new Record(key, value, System.currentTimeMillis());
            wal.append(record);
            activeMemTable.put(record);
            
            if (activeMemTable.getSizeInBytes() >= memTableThreshold) {
                rotateMemTable();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(byte[] key) throws IOException {
        lock.writeLock().lock();
        try {
            Record record = Record.tombstone(key, System.currentTimeMillis());
            wal.append(record);
            activeMemTable.put(record);
            
            if (activeMemTable.getSizeInBytes() >= memTableThreshold) {
                rotateMemTable();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] get(byte[] key) throws IOException {
        // 1. Check active MemTable
        Record record = activeMemTable.get(key);
        if (record != null) return record.isTombstone() ? null : record.getValue();
        
        // 2. Check immutable MemTables
        for (MemTable mt : immutableMemTables) {
            record = mt.get(key);
            if (record != null) return record.isTombstone() ? null : record.getValue();
        }
        
        lock.readLock().lock();
        try {
            // 3. Check SSTables (latest first)
            for (int i = ssTables.size() - 1; i >= 0; i--) {
                record = ssTables.get(i).get(key);
                if (record != null) return record.isTombstone() ? null : record.getValue();
            }
        } finally {
            lock.readLock().unlock();
        }
        
        return null;
    }

    private void rotateMemTable() throws IOException {
        final MemTable oldTable = activeMemTable;
        immutableMemTables.addFirst(oldTable);
        activeMemTable = new MemTable();
        
        // New WAL for the new active memtable
        Path oldWalPath = dataDir.resolve("wal.log");
        Path archivedWalPath = dataDir.resolve("wal_" + System.currentTimeMillis() + ".log");
        wal.close();
        Files.move(oldWalPath, archivedWalPath);
        wal = new WAL(oldWalPath);
        
        flushExecutor.submit(() -> {
            try {
                flush(oldTable, archivedWalPath);
            } catch (IOException e) {
                logger.error("Flush failed", e);
            }
        });
    }

    private synchronized void flush(MemTable table, Path walPath) throws IOException {
        String fileName = "sstable_" + System.currentTimeMillis() + ".db";
        Path ssTablePath = dataDir.resolve(fileName);
        SSTable ssTable = new SSTable(ssTablePath, table.getAll(), bloomFilterFpr);
        
        lock.writeLock().lock();
        try {
            ssTables.add(ssTable);
            immutableMemTables.remove(table);
        } finally {
            lock.writeLock().unlock();
        }
        Files.deleteIfExists(walPath);
        logger.info("Flushed MemTable to SSTable: {}", fileName);
    }

    private void recover() throws IOException {
        // Replay current WAL
        List<Record> records = wal.replay();
        for (Record r : records) {
            activeMemTable.put(r);
        }
        
        // Replay archived WALs if any (crashed during flush)
        File[] archivedWals = dataDir.toFile().listFiles((dir, name) -> name.startsWith("wal_") && name.endsWith(".log"));
        if (archivedWals != null) {
            for (File f : archivedWals) {
                WAL archivedWal = new WAL(f.toPath());
                List<Record> archivedRecords = archivedWal.replay();
                MemTable mt = new MemTable();
                for (Record r : archivedRecords) {
                    mt.put(r);
                }
                if (!mt.isEmpty()) {
                    flush(mt, f.toPath());
                } else {
                    f.delete();
                }
            }
        }
    }

    private void loadSSTables() throws IOException {
        File[] files = dataDir.toFile().listFiles((dir, name) -> name.startsWith("sstable_") && name.endsWith(".db"));
        if (files != null) {
            Arrays.sort(files, Comparator.comparingLong(File::lastModified));
            for (File f : files) {
                ssTables.add(SSTable.load(f.toPath()));
            }
        }
    }

    public void close() throws IOException {
        compactionExecutor.shutdown();
        flushExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(30, TimeUnit.SECONDS);
            flushExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        wal.close();
    }
}
