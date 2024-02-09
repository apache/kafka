/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
