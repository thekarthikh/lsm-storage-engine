package com.lsm.core;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class BloomFilter implements Serializable {
    private final int numHashFunctions;
    private final BitSet bitSet;
    private final int size;

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        this.size = optimalNumOfBits(expectedInsertions, falsePositiveRate);
        this.numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, size);
        this.bitSet = new BitSet(size);
    }

    private BloomFilter(int size, int numHashFunctions, BitSet bitSet) {
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

    private static int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
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
        byte[] bytes = new byte[bytesLength];
        dis.readFully(bytes);
        return new BloomFilter(size, numHashFunctions, BitSet.valueOf(bytes));
    }
}
