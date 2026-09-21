package com.lsm.demo;

import com.lsm.core.StorageEngine;
import java.nio.charset.StandardCharsets;

/** Small deterministic command-line demonstration of the real storage-engine API. */
public final class LsmDemo {
    private LsmDemo() { }
    public static void main(String[] args) throws Exception {
        String dataDir = args.length == 0 ? "data" : args[0];
        byte[] survivor = bytes("demo:survivor"); byte[] deleted = bytes("demo:deleted");
        try (StorageEngine engine = new StorageEngine(dataDir, 64 * 1024, 0.01)) {
            engine.put(survivor, bytes("value-v1")); engine.put(survivor, bytes("value-v2"));
            System.out.println("GET after update: " + text(engine.get(survivor)));
            engine.delete(deleted); System.out.println("GET after delete: " + text(engine.get(deleted)));
            engine.flush(); engine.put(survivor, bytes("value-v3")); engine.flush(); engine.compact();
        }
        try (StorageEngine reopened = new StorageEngine(dataDir, 64 * 1024, 0.01)) {
            System.out.println("GET after reopen: " + text(reopened.get(survivor)));
            System.out.println("DELETED after reopen: " + text(reopened.get(deleted)));
        }
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(byte[] value) { return value == null ? "<missing>" : new String(value, StandardCharsets.UTF_8); }
}
