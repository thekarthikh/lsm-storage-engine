package com.lsm.core;

import com.lsm.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/** Merges tables supplied oldest-to-newest. Equal timestamps prefer the newer table. */
public class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    private final Path dataDir; private final double bloomFilterFpr;
    public CompactionManager(Path dataDir, double bloomFilterFpr) { this.dataDir = dataDir; this.bloomFilterFpr = bloomFilterFpr; }
    public SSTable compact(List<SSTable> tables) throws IOException {
        if (tables.isEmpty()) return null; if (tables.size() == 1) return tables.get(0);
        Map<byte[], Record> merged = new TreeMap<>(Arrays::compare);
        for (SSTable table : tables) for (Record candidate : table.scan()) {
            Record current = merged.get(candidate.getKey());
            if (current == null || candidate.getTimestamp() >= current.getTimestamp()) merged.put(candidate.getKey(), candidate);
        }
        ConcurrentSkipListMap<byte[], Record> data = new ConcurrentSkipListMap<>(Arrays::compare); data.putAll(merged);
        Path path = dataDir.resolve("sstable_compacted_" + UUID.randomUUID() + ".db");
        SSTable result = new SSTable(path, data, bloomFilterFpr);
        logger.info("Compacted {} SSTables into {}", tables.size(), path.getFileName());
        return result;
    }
}
