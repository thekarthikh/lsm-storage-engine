# LSM Tree Storage Engine in Java

A high-performance Log-Structured Merge-tree (LSM-tree) storage engine implemented in Java. Designed for high write throughput and crash safety.

## Architecture

### 1. Write Path
- **WAL (Write-Ahead Log)**: Every write is first appended to a persistent log for durability. In case of a crash, the WAL is replayed to restore the in-memory state.
- **MemTable**: An in-memory sorted structure (implemented using `ConcurrentSkipListMap`). It provides $O(\log N)$ search and insertion.
- **Flush Mechanism**: When the MemTable reaches a size threshold (e.g., 64MB), it becomes immutable and is rotated. A background thread flushes it to disk as a new SSTable.

### 2. Read Path
- **MemTable Check**: Search in the active and immutable MemTables first.
- **Bloom Filter**: Each SSTable has an associated Bloom Filter in its index. If the filter indicates a key is NOT present, we skip the expensive disk I/O.
- **SSTable Search**: If the Bloom Filter passes, we use the SSTable index to jump to the data block and retrieve the record.

### 3. Persistence Layer (SSTable)
- **Immutable Files**: Once written, SSTables are never modified.
- **Internal Structure**:
  - **Data Blocks**: Sorted records.
  - **Index**: Key-to-offset mapping for fast lookups.
  - **Bloom Filter**: Probabilistic filter to skip unnecessary reads.
  - **Footer**: Metadata containing offsets to the index and bloom filter.

### 4. Compaction
- **Size-Tiered Compaction**: Merges multiple small SSTables into larger ones to reduce the number of files and improve read performance.
- **Tombstones**: Deletions are handled via tombstones, which are purged during compaction.

## Performance
- **Write Throughput**: Targeted at 100K+ ops/sec.
- **Read Latency**: Minimized via Bloom Filters and efficient indexing.
- **Durability**: Guaranteed via `fsync` on the WAL.

## Benchmarks
Run the JMH benchmarks using:
```bash
mvn clean package
java -jar target/benchmarks.jar
```

## Features
- **Crash Safety**: Full WAL-backed recovery.
- **Concurrency**: Thread-safe MemTable and multi-threaded flush/compaction.
- **Memory Efficiency**: Streaming merge during compaction to handle large datasets.
