# LSM-Tree Storage Engine

This is a Java implementation of an embedded LSM-tree storage engine demonstrating write-ahead logging, MemTables, immutable MemTables, SSTables, Bloom filters, tombstones, compaction, concurrency, and restart recovery. It is an interview-focused systems project, not a production database.

## What This Project Demonstrates

- Write-ahead logging with forced channel writes.
- MemTable and immutable MemTable flushing.
- Immutable SSTable persistence with metadata and indexes.
- Bloom-filter-assisted point lookups.
- Updates and deletes represented with tombstones.
- SSTable compaction and newest-version resolution.
- Restart/recovery and concurrent reads/writes.
- Structural validation of persisted data.
- Dockerized execution with persistent storage.

**Verified automated result:** 9 tests pass, 0 failures, 0 errors.

## Architecture

```text
Application
    |
    v
   WAL
    |
    v
 MemTable
    |
    v
Immutable MemTable
    |
    v
 SSTable
    |------> Bloom Filter
    |------> Index
    v
Compaction
```

The WAL records writes before MemTable visibility. Rotated MemTables become immutable and are flushed. SSTables are immutable sorted files with an index, Bloom filter, and footer metadata. Reads merge the active MemTable, immutable MemTables, and newest-to-oldest SSTables.

## Core Components

### WAL

Writes are appended to bounded, length-framed records and the channel is forced before MemTable publication. Replay validates lengths, flags, and trailing bytes. An incomplete final record is truncated while preceding valid records survive. A malformed complete record fails closed.

### MemTable

The MemTable is a byte-array ordered ConcurrentSkipListMap. Updates replace the visible record for a key. DELETE stores a tombstone. Rotation creates an immutable MemTable and a new active table.

### SSTables

SSTables are written to a temporary path, flushed and synced, then moved into place atomically when supported. They contain sorted records, a key-to-offset index, a Bloom filter, header/footer markers, creation time, and maximum record timestamp.

### Bloom Filter

The Bloom filter avoids unnecessary SSTable reads for keys definitely absent. False positives are possible and safely fall through to the index and record lookup.

### Tombstones

DELETE is represented by a tombstone. Tombstones hide older values and prevent deleted keys from reappearing. They are retained during compaction because this engine has no global deletion horizon.

### Compaction

Compaction merges SSTables oldest-to-newest, keeps the greatest timestamp for each key, resolves equal timestamps in favor of the newer table, and publishes a new immutable SSTable before deleting obsolete inputs.

### Recovery

The database can be closed and reopened on the same directory. Startup loads validated SSTables, replays the active WAL, and recovers archived WALs from interrupted rotation.

## Quick Start

Requirements: Java 11+, Maven 3.9+, and optionally Docker.

```powershell
git clone https://github.com/thekarthikh/lsm-storage-engine.git
cd lsm-storage-engine
mvn clean test
mvn package
```

**Verified result:** 9 tests, 0 failures, 0 errors, Maven `BUILD SUCCESS`.

## Docker Quick Start

```powershell
docker build -t lsm-tree .
docker volume create lsm-data
docker run --rm -v lsm-data:/app/data lsm-tree
```

The container runs `com.lsm.demo.LsmDemo`, which uses the real `StorageEngine` API. The named volume preserves WAL and SSTables across separate container runs; this is local persistence, not distributed persistence.

### Verified Docker Behavior

```text
GET after update: value-v2
GET after delete: <missing>
GET after reopen: value-v3
DELETED after reopen: <missing>
```

## Demo

`com.lsm.demo.LsmDemo` exercises:

1. PUT
2. UPDATE
3. GET
4. DELETE
5. FLUSH
6. COMPACTION
7. CLOSE
8. REOPEN
9. GET after recovery

## Testing

```powershell
mvn clean test
mvn package
git diff --check
```

**Verified result:** 9 tests, 0 failures, 0 errors; Maven package succeeds.

Coverage includes WAL, recovery, flush, compaction, tombstones, concurrent readers/writers, malformed/truncated WAL input, corrupted SSTables, restart ordering, and empty/unusual values.

## Benchmark

`com.lsm.benchmark.StorageEngineBenchmark` is packaged as `target/benchmarks.jar` after `mvn package`. `testWrite` uses random 16-byte keys and random 100-byte values and measures `StorageEngine.put`, including WAL force and MemTable work. `testRead` measures random-key misses, not hit latency.

Recorded local smoke measurement: JDK 11 on Windows, 1 fork, 1 thread, with the benchmark's configured warmup/measurement settings.

- Put: **1,656.740 ops/s**
- Random-miss get: **31,278,554.650 ops/s**

These are environment-specific smoke measurements, not production throughput or portability claims.

## Technology Stack

Java 11 · Maven · JUnit 5 · JMH · Docker

## Project Structure

```text
src/
├── main/java/com/lsm/
│   ├── core/       StorageEngine, WAL, MemTable, SSTable, BloomFilter, CompactionManager
│   ├── model/      Record
│   ├── benchmark/  StorageEngineBenchmark
│   └── demo/       LsmDemo
└── test/java/com/lsm/core/StorageEngineTest.java

Dockerfile
.dockerignore
.gitignore
README.md
pom.xml
```

## Engineering Decisions

- WAL-before-MemTable ordering provides a persisted recovery record before in-memory visibility.
- Immutable SSTables let readers share published files while replacements are created.
- Tombstone deletes remain visible over older SSTables.
- Compaction resolves obsolete versions while preserving the newest record.
- Persisted SSTable ordering metadata avoids filename-dependent restart ordering.
- Read/write locking and single maintenance executors coordinate publication.
- Bounded validation limits malformed persisted-data allocations.
- Atomic publication is used where supported.

## Limitations

The project does not provide:

- Distributed replication or consensus
- Transactions
- Snapshots
- A manifest/version-set crash protocol
- WAL/SSTable checksums
- Compression
- A block cache
- A sparse/block-based index
- Multi-process coordination
- Hardware power-loss validation
- Large-scale production stress testing

These are outside the current project's scope.

## Interview Quick Demo

**Target length: approximately 2–3 minutes.**

1. Run `mvn clean test`.
2. Build the Docker image.
3. Run the Docker demo with a persistent volume.
4. Explain WAL → MemTable → immutable MemTable → SSTable.
5. Demonstrate UPDATE and newest-value resolution.
6. Demonstrate DELETE and tombstone visibility.
7. Explain compaction and immutable-file publication.
8. Run the container again using the same volume.
9. Show the recovered value and tombstone.
