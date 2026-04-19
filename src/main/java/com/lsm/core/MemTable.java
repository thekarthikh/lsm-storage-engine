package com.lsm.core;

import com.lsm.model.Record;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.Arrays;

public class MemTable {
    private final ConcurrentSkipListMap<byte[], Record> map;
    private final LongAdder sizeInBytes;

    public MemTable() {
        this.map = new ConcurrentSkipListMap<>(new ByteArrayComparator());
        this.sizeInBytes = new LongAdder();
    }

    public void put(Record record) {
        Record old = map.put(record.getKey(), record);
        int added = record.getKey().length + (record.isTombstone() ? 0 : record.getValue().length) + 16;
        if (old != null) {
            int removed = old.getKey().length + (old.isTombstone() ? 0 : old.getValue().length) + 16;
            sizeInBytes.add(added - removed);
        } else {
            sizeInBytes.add(added);
        }
    }

    public Record get(byte[] key) {
        return map.get(key);
    }

    public ConcurrentSkipListMap<byte[], Record> getAll() {
        return map;
    }

    public long getSizeInBytes() {
        return sizeInBytes.sum();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    private static class ByteArrayComparator implements Comparator<byte[]> {
        @Override
        public int compare(byte[] a, byte[] b) {
            return Arrays.compare(a, b);
        }
    }
}
