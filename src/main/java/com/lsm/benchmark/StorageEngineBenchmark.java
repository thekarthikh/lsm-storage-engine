package com.lsm.benchmark;

import com.lsm.core.StorageEngine;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StorageEngineBenchmark {

    private StorageEngine engine;
    private final Random random = new Random();
    private Path tempDir;

    @Setup
    public void setup() throws IOException {
        tempDir = Paths.get("bench_data");
        if (Files.exists(tempDir)) {
            deleteDirectory(tempDir.toFile());
        }
        engine = new StorageEngine("bench_data", 64 * 1024 * 1024, 0.01);
    }

    @TearDown
    public void tearDown() throws IOException {
        engine.close();
        deleteDirectory(tempDir.toFile());
    }

    @Benchmark
    public void testWrite() throws IOException {
        byte[] key = new byte[16];
        byte[] value = new byte[100];
        random.nextBytes(key);
        random.nextBytes(value);
        engine.put(key, value);
    }

    @Benchmark
    public void testRead() throws IOException {
        byte[] key = new byte[16];
        // Note: Reads will mostly miss since keys are random, testing Bloom Filter efficiency
        engine.get(key);
    }

    private void deleteDirectory(java.io.File file) {
        java.io.File[] contents = file.listFiles();
        if (contents != null) {
            for (java.io.File f : contents) {
                deleteDirectory(f);
            }
        }
        file.delete();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StorageEngineBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
