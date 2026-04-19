package com.lsm.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Record {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private boolean tombstone;

    public Record(byte[] key, byte[] value, long timestamp) {
        this(key, value, timestamp, false);
    }

    public static Record tombstone(byte[] key, long timestamp) {
        return new Record(key, null, timestamp, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Arrays.equals(key, record.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
