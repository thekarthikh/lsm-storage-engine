package com.lsm.core;

import com.lsm.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class StorageEngineTest {
    @TempDir Path temp;

    @Test void walRoundTripAndTruncatedTail() throws Exception {
        Path file = temp.resolve("wal.log");
        try (WAL wal = new WAL(file)) { wal.append(new Record(b("a"), b("1"), 1)); wal.append(Record.tombstone(b("b"), 2)); }
        long validLength = Files.size(file); Files.write(file, new byte[]{0, 0, 0, 20, 1}, StandardOpenOption.APPEND);
        try (WAL wal = new WAL(file)) { assertEquals(2, wal.replay().size()); }
        assertEquals(validLength, Files.size(file));
    }

    @Test void walRejectsInvalidLength() throws Exception {
        Path file = temp.resolve("wal.log");
        Files.write(file, new byte[]{0, 0, 0, 1});
        try (WAL wal = new WAL(file)) { assertThrows(IOException.class, wal::replay); }
    }

    @Test void writesUpdatesDeletesAndRestartRecovery() throws Exception {
        Path dir = temp.resolve("db");
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            engine.put(b("k"), b("v1")); engine.put(b("k"), b("v2")); assertArrayEquals(b("v2"), engine.get(b("k")));
            engine.delete(b("k")); assertNull(engine.get(b("k"))); engine.put(b("k"), b("v3")); assertArrayEquals(b("v3"), engine.get(b("k")));
        }
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) { assertArrayEquals(b("v3"), engine.get(b("k"))); }
    }

    @Test void flushRestartAndCompactionPreserveTombstones() throws Exception {
        Path dir = temp.resolve("db");
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            for (int i = 0; i < 5; i++) { engine.put(b("key"), b("v" + i)); engine.flush(); }
            engine.delete(b("key")); engine.flush();
            assertNull(engine.get(b("key"))); engine.compact(); assertNull(engine.get(b("key")));
        }
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) { assertNull(engine.get(b("key"))); }
    }

    @Test void restartOrdersMultipleSstablesByPersistedVersion() throws Exception {
        Path dir = temp.resolve("ordered-db");
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            engine.put(b("key"), b("old")); engine.flush();
            engine.put(b("key"), b("new")); engine.flush();
        }
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            assertArrayEquals(b("new"), engine.get(b("key")));
        }
    }

    @Test void compactionAfterRestartKeepsNewestValue() throws Exception {
        Path dir = temp.resolve("compact-restart-db");
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            engine.put(b("key"), b("v1")); engine.flush();
            engine.put(b("key"), b("v2")); engine.flush();
        }
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            assertArrayEquals(b("v2"), engine.get(b("key")));
            engine.compact();
            assertArrayEquals(b("v2"), engine.get(b("key")));
        }
    }

    @Test void emptyValuesAndUnusualKeysAreStored() throws Exception {
        Path dir = temp.resolve("edge-db");
        byte[] key = new byte[]{0, -1, 42};
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            engine.put(key, new byte[0]);
            assertArrayEquals(new byte[0], engine.get(key));
            engine.flush();
            assertArrayEquals(new byte[0], engine.get(key));
        }
    }

    @Test void concurrentReadersAndWriters() throws Exception {
        Path dir = temp.resolve("db");
        try (StorageEngine engine = new StorageEngine(dir.toString(), 1024 * 1024, .01)) {
            ExecutorService pool = Executors.newFixedThreadPool(8); List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) futures.add(pool.submit(() -> { try { for (int i = 0; i < 100; i++) engine.put(b("k" + i), b("v" + i)); } catch (IOException e) { throw new UncheckedIOException(e); } }));
            for (int t = 0; t < 4; t++) futures.add(pool.submit(() -> { try { for (int i = 0; i < 100; i++) engine.get(b("k" + i)); } catch (IOException e) { throw new UncheckedIOException(e); } }));
            for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS); pool.shutdown();
            for (int i = 0; i < 100; i++) assertArrayEquals(b("v" + i), engine.get(b("k" + i)));
        }
    }

    @Test void corruptedSstableIsRejected() throws Exception {
        Path file = temp.resolve("bad.db"); Files.write(file, new byte[]{1, 2, 3});
        assertThrows(IOException.class, () -> SSTable.load(file));
    }

    private static byte[] b(String value) { return value.getBytes(java.nio.charset.StandardCharsets.UTF_8); }
}
