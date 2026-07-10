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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link ProducerMetadata#add} against a baseline that mirrors the prior, fully
 * {@code synchronized} implementation (map update + newTopics bookkeeping as a single
 * {@code synchronized} block). Every {@code send()} call refreshes the expiry of the topic
 * it's producing to via {@code add()}, so with many application threads sharing one producer,
 * this refresh is on the hot path and contends across threads for a topic that's virtually
 * always already tracked. This benchmark simulates that: many threads concurrently refreshing
 * a shared pool of already-registered topics.
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
    private ProducerMetadata lockFreeHotPathMetadata;
    private FullySynchronizedProducerMetadata fullySynchronizedMetadata;

    @Setup(Level.Trial)
    public void setup() {
        topics = new String[TOPIC_COUNT];
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topics[i] = "topic-" + i;
        }

        lockFreeHotPathMetadata = new ProducerMetadata(100L, 1000L, TimeUnit.MINUTES.toMillis(5), METADATA_IDLE_MS,
            new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
        fullySynchronizedMetadata = new FullySynchronizedProducerMetadata(METADATA_IDLE_MS);

        long nowMs = Time.SYSTEM.milliseconds();
        for (String topic : topics) {
            lockFreeHotPathMetadata.add(topic, nowMs);
            fullySynchronizedMetadata.add(topic, nowMs);
        }
    }

    @Benchmark
    public void addExistingTopicFullySynchronized() {
        String topic = topics[ThreadLocalRandom.current().nextInt(TOPIC_COUNT)];
        fullySynchronizedMetadata.add(topic, Time.SYSTEM.milliseconds());
    }

    @Benchmark
    public void addExistingTopicLockFreeHotPath() {
        String topic = topics[ThreadLocalRandom.current().nextInt(TOPIC_COUNT)];
        lockFreeHotPathMetadata.add(topic, Time.SYSTEM.milliseconds());
    }

    /**
     * Baseline mirroring the prior {@code ProducerMetadata#add}: a plain {@code HashMap}
     * guarded end-to-end by a single {@code synchronized} method, so every call - including a
     * refresh of an already-tracked topic - serializes on the instance lock.
     */
    private static class FullySynchronizedProducerMetadata {
        private final long metadataIdleMs;
        private final Map<String, Long> topics = new HashMap<>();
        private final Set<String> newTopics = new HashSet<>();

        FullySynchronizedProducerMetadata(long metadataIdleMs) {
            this.metadataIdleMs = metadataIdleMs;
        }

        synchronized void add(String topic, long nowMs) {
            Objects.requireNonNull(topic, "topic cannot be null");
            if (topics.put(topic, nowMs + metadataIdleMs) == null) {
                newTopics.add(topic);
            }
        }
    }
}
