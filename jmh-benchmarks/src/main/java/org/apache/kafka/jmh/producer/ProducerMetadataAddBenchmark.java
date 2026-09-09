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

package org.apache.kafka.jmh.producer;

import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link ProducerMetadata#add} refreshing an already-tracked topic's expiry. Every
 * {@code send()} call does this refresh, so with many application threads sharing one producer,
 * it's on the hot path and contends across threads for a topic that's virtually always already
 * tracked. This benchmark simulates that: many threads concurrently refreshing a shared pool of
 * already-registered topics.
 *
 * To measure the effect of a change to {@code add()}, run this benchmark before and after the
 * change and compare the throughput, rather than hand-mirroring the old implementation here
 * where it could drift out of sync with reality.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(8)
public class ProducerMetadataAddBenchmark {

    private static final int TOPIC_COUNT = 200;
    private static final long METADATA_IDLE_MS = TimeUnit.MINUTES.toMillis(5);

    private String[] topics;
    private ProducerMetadata metadata;

    @Setup(Level.Trial)
    public void setup() {
        topics = new String[TOPIC_COUNT];
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topics[i] = "topic-" + i;
        }

        metadata = new ProducerMetadata(100L, 1000L, TimeUnit.MINUTES.toMillis(5), METADATA_IDLE_MS,
            new LogContext(), new ClusterResourceListeners());

        long nowMs = Time.SYSTEM.milliseconds();
        for (String topic : topics) {
            metadata.add(topic, nowMs);
        }
    }

    @Benchmark
    public void addExistingTopic() {
        String topic = topics[ThreadLocalRandom.current().nextInt(TOPIC_COUNT)];
        metadata.add(topic, Time.SYSTEM.milliseconds());
    }
}
