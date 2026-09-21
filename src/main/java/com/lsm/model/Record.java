package com.lsm.model;

import java.util.Arrays;

public class Record {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private boolean tombstone;

    public Record() { this(new byte[0], new byte[0], 0L, false); }

    public Record(byte[] key, byte[] value, long timestamp, boolean tombstone) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (!tombstone && value == null) throw new IllegalArgumentException("value must not be null");
        this.key = Arrays.copyOf(key, key.length);
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    public Record(byte[] key, byte[] value, long timestamp) { this(key, value, timestamp, false); }

    public byte[] getKey() { return Arrays.copyOf(key, key.length); }
    public void setKey(byte[] key) { this.key = Arrays.copyOf(key, key.length); }
    public byte[] getValue() { return value == null ? null : Arrays.copyOf(value, value.length); }
    public void setValue(byte[] value) { this.value = value == null ? null : Arrays.copyOf(value, value.length); }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public boolean isTombstone() { return tombstone; }
    public void setTombstone(boolean tombstone) { this.tombstone = tombstone; }

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
