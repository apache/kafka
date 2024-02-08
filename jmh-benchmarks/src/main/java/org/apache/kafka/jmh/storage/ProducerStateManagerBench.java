package org.apache.kafka.jmh.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@State(value = Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProducerStateManagerBench {
    Time time = new MockTime();
    final int producerIdExpirationMs = 1000;

    ProducerStateManager manager;
    Path tempDirectory;

    @Param({"100", "1000", "10000", "100000"})
    public int numProducerIds;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDirectory = Files.createTempDirectory("kafka-logs");
        manager = new ProducerStateManager(
            new TopicPartition("t1", 0),
            tempDirectory.toFile(),
            Integer.MAX_VALUE,
            new ProducerStateManagerConfig(producerIdExpirationMs, false),
            time
        );
    }


    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Files.deleteIfExists(tempDirectory);
    }

    @Benchmark
    @Threads(1)
    public void testDeleteExpiringIds() {
        short epoch = 0;
        for (long i = 0L; i < numProducerIds; i++) {
            final ProducerStateEntry entry = new ProducerStateEntry(
                i,
                epoch,
                0,
                time.milliseconds(),
                OptionalLong.empty(),
                Optional.empty()
            );
            manager.loadProducerEntry(entry);
        }

        manager.removeExpiredProducers(time.milliseconds() + producerIdExpirationMs + 1);
    }
}
