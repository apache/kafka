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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.TimestampedKeyValueStoreMaterializer;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * This is a simple example of a JMH benchmark.
 * <p>
 * The sample code provided by the JMH project is a great place to start learning how to write correct benchmarks:
 * http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StreamsStoreBenchmark {

    private static final int DISTINCT_KEYS = 10_000;

    private static final String KEY = "the_key_to_use";

    private static final String VALUE = "the quick brown fox jumped over the lazy dog the olympics are about to start";

    private final String[] keys = new String[DISTINCT_KEYS];

    @SuppressWarnings("unchecked")
    private final ValueAndTimestamp<String>[] values = new ValueAndTimestamp[DISTINCT_KEYS];


    private int counter;

    private KeyValueStore<String, ValueAndTimestamp<String>> persistentTimestampedStore;
    private KeyValueStore<String, ValueAndTimestamp<String>> persistentTimestampedStoreNoCache;
    private KeyValueStore<String, ValueAndTimestamp<String>> inMemoryTimestampedStore;
    private KeyValueStore<String, ValueAndTimestamp<String>> inMemoryTimestampedStoreNoCache;

    @Setup(Level.Trial)
    public void setUp() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = KEY + i;
            values[i] = ValueAndTimestamp.make(VALUE + i, i);
        }

        persistentTimestampedStore =
            new TimestampedKeyValueStoreMaterializer<>(
                new MaterializedInternal<>(
                    Materialized
                        .<String, String, KeyValueStore<Bytes, byte[]>>as("rocks")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withLoggingDisabled()
                )
            ).materialize()
             .build();

        persistentTimestampedStoreNoCache =
            new TimestampedKeyValueStoreMaterializer<>(
                new MaterializedInternal<>(
                    Materialized
                        .<String, String, KeyValueStore<Bytes, byte[]>>as("rocksnc")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled()
                        .withLoggingDisabled()
                )
            ).materialize()
             .build();

        inMemoryTimestampedStore =
            new TimestampedKeyValueStoreMaterializer<>(
                new MaterializedInternal<>(
                    Materialized
                        .<String, String>as(Stores.inMemoryKeyValueStore("mem"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withLoggingDisabled()
                )
            ).materialize()
             .build();

        inMemoryTimestampedStoreNoCache =
            new TimestampedKeyValueStoreMaterializer<>(
                new MaterializedInternal<>(
                    Materialized
                        .<String, String>as(Stores.inMemoryKeyValueStore("memnc"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled()
                        .withLoggingDisabled()
                )
            ).materialize()
             .build();

        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(), "test-metrics");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1_000_000_000, metrics);
        final StreamsConfig config = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test")
        ));
        final TaskId id = new TaskId(0, 0);
        final ProcessorStateManager stateMgr;
        try {
            stateMgr = new ProcessorStateManager(
                id,
                Collections.emptySet(),
                false,
                new StateDirectory(config, Time.SYSTEM, true),
                Collections.emptyMap(),
                null,
                false,
                new LogContext("jmh")
            );
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        final ProcessorContextImpl context = new ProcessorContextImpl(
            id,
            null,
            config,
            null,
            stateMgr,
            metrics,
            cache
        );

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        persistentTimestampedStore.init(context, persistentTimestampedStore);
        persistentTimestampedStoreNoCache.init(context, persistentTimestampedStoreNoCache);
        inMemoryTimestampedStore.init(context, inMemoryTimestampedStore);
        inMemoryTimestampedStoreNoCache.init(context, inMemoryTimestampedStoreNoCache);
    }

    @Benchmark
    public ValueAndTimestamp<String> testPersistentPutGetPerformance() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        persistentTimestampedStore.put(key, values[index]);

        return persistentTimestampedStore.get(key);
    }


    @Benchmark
    public ValueAndTimestamp<String> testPersistentPutGetPerformanceNoCache() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        persistentTimestampedStoreNoCache.put(key, values[index]);

        return persistentTimestampedStoreNoCache.get(key);
    }

    @Benchmark
    public ValueAndTimestamp<String> testMemoryPutGetPerformance() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        inMemoryTimestampedStore.put(key, values[index]);

        return inMemoryTimestampedStore.get(key);
    }

    @Benchmark
    public ValueAndTimestamp<String> testMemoryPutGetPerformanceNoCache() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        inMemoryTimestampedStoreNoCache.put(key, values[index]);

        return inMemoryTimestampedStoreNoCache.get(key);
    }

    public static void main(final String[] args) {
        final StreamsStoreBenchmark streamsCacheBenchmark = new StreamsStoreBenchmark();
        streamsCacheBenchmark.setUp();
        System.out.println(streamsCacheBenchmark.testPersistentPutGetPerformance());
        System.out.println(streamsCacheBenchmark.testPersistentPutGetPerformanceNoCache());
        System.out.println(streamsCacheBenchmark.testMemoryPutGetPerformance());
        System.out.println(streamsCacheBenchmark.testMemoryPutGetPerformanceNoCache());
    }

}
