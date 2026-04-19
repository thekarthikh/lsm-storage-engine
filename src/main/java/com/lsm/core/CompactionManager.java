package com.lsm.core;

import com.lsm.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    private final Path dataDir;
    private final double bloomFilterFpr;

    public CompactionManager(Path dataDir, double bloomFilterFpr) {
        this.dataDir = dataDir;
        this.bloomFilterFpr = bloomFilterFpr;
    }

    public SSTable compact(List<SSTable> tables) throws IOException {
        if (tables.isEmpty()) return null;
        if (tables.size() == 1) return tables.get(0);

        logger.info("Compacting {} SSTables", tables.size());

        // Multi-way merge sort
        PriorityQueue<PeekingIterator> pq = new PriorityQueue<>((a, b) -> 
            Arrays.compare(a.peek().getKey(), b.peek().getKey()));

        for (SSTable table : tables) {
            Iterator<Record> it = table.scan().iterator();
            if (it.hasNext()) {
                pq.add(new PeekingIterator(it));
            }
        }

        ConcurrentSkipListMap<byte[], Record> mergedData = new ConcurrentSkipListMap<>(Arrays::compare);

        while (!pq.isEmpty()) {
            PeekingIterator smallest = pq.poll();
            Record record = smallest.next();
            
            // Check if other iterators have the same key, pick the one with later timestamp
            while (!pq.isEmpty() && Arrays.equals(pq.peek().peek().getKey(), record.getKey())) {
                PeekingIterator other = pq.poll();
                Record otherRecord = other.next();
                if (otherRecord.getTimestamp() > record.getTimestamp()) {
                    record = otherRecord;
                }
                if (other.hasNext()) pq.add(other);
            }
            
            mergedData.put(record.getKey(), record);
            if (smallest.hasNext()) pq.add(smallest);
        }

        String fileName = "sstable_compacted_" + System.currentTimeMillis() + ".db";
        Path path = dataDir.resolve(fileName);
        SSTable result = new SSTable(path, mergedData, bloomFilterFpr);

        // Delete old tables
        for (SSTable table : tables) {
            Files.deleteIfExists(table.getPath());
        }

        logger.info("Compaction finished: {}", fileName);
        return result;
    }

    private static class PeekingIterator implements Iterator<Record> {
        private final Iterator<Record> it;
        private Record next;

        public PeekingIterator(Iterator<Record> it) {
            this.it = it;
            if (it.hasNext()) next = it.next();
        }

        public Record peek() {
            return next;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Record next() {
            Record current = next;
            next = it.hasNext() ? it.next() : null;
            return current;
        }
    }
}
