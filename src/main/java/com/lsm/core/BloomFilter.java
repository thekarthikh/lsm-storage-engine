package com.lsm.core;

import java.io.*;
import java.util.BitSet;

public class BloomFilter implements Serializable {
    private static final int MAX_BITS = 1 << 27;
    private final int numHashFunctions;
    private final BitSet bitSet;
    private final int size;

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions < 0 || Double.isNaN(falsePositiveRate)
                || falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            throw new IllegalArgumentException("invalid Bloom filter parameters");
        }
        long calculatedBits = optimalNumOfBits(expectedInsertions, falsePositiveRate);
        if (calculatedBits <= 0 || calculatedBits > MAX_BITS) throw new IllegalArgumentException("Bloom filter is too large");
        this.size = (int) calculatedBits;
        this.numHashFunctions = optimalNumOfHashFunctions(Math.max(1, expectedInsertions), size);
        this.bitSet = new BitSet(size);
    }

    private BloomFilter(int size, int numHashFunctions, BitSet bitSet) {
        if (size <= 0 || numHashFunctions <= 0) throw new IllegalArgumentException("invalid Bloom filter metadata");
        this.size = size;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = bitSet;
    }

    public void add(byte[] key) {
        int hash1 = hash(key, 0);
        int hash2 = hash(key, hash1);
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) combinedHash = ~combinedHash;
            bitSet.set(combinedHash % size);
        }
    }

    public boolean mightContain(byte[] key) {
        int hash1 = hash(key, 0);
        int hash2 = hash(key, hash1);
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) combinedHash = ~combinedHash;
            if (!bitSet.get(combinedHash % size)) {
                return false;
            }
        }
        return true;
    }

    private int hash(byte[] data, int seed) {
        int h = seed ^ 0x811c9dc5;
        for (byte b : data) {
            h = (h ^ (b & 0xff)) * 0x01000193;
        }
        return h;
    }

    private static long optimalNumOfBits(long n, double p) {
        double bits = -n * Math.log(p) / (Math.log(2) * Math.log(2));
        return bits > Long.MAX_VALUE ? Long.MAX_VALUE : (long) Math.ceil(bits);
    }

    private static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(size);
        dos.writeInt(numHashFunctions);
        byte[] bytes = bitSet.toByteArray();
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public static BloomFilter deserialize(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int size = dis.readInt();
        int numHashFunctions = dis.readInt();
        int bytesLength = dis.readInt();
        if (size <= 0 || numHashFunctions <= 0 || bytesLength < 0 || bytesLength > (size + 7) / 8) {
            throw new IOException("invalid Bloom filter metadata");
        }
        byte[] bytes = new byte[bytesLength];
        dis.readFully(bytes);
        return new BloomFilter(size, numHashFunctions, BitSet.valueOf(bytes));
    }
}
