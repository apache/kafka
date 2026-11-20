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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that record caches and suppress buffers keep no source raw records without a dead-letter queue, and
 * that with one, {@code statestore.cache.max.bytes} bounds the raw bytes they keep. Each topology stores a tiny
 * value derived from a large raw input. Each raw input array is tracked with a weak reference, so an array still
 * reachable after garbage collection is one the topology still holds.
 */
public class RecordCacheSourceRawRecordMemoryTest {

    private static final String INPUT_TOPIC = "input";
    private static final String DEAD_LETTER_QUEUE_TOPIC = "dlq";
    private static final int RAW_VALUE_SIZE_BYTES = 40_000;
    private static final long CACHE_MAX_BYTES = 1024L * 1024L;
    // 400 x 40 KB is about 16 MB of raw input, roughly 15 times the cache budget
    private static final int DISTINCT_KEYS = 400;
    // headroom for what the size estimate leaves out (object headers, map nodes)
    private static final double CACHE_OVERHEAD_FACTOR = 1.25d;
    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final int GARBAGE_COLLECTION_ATTEMPTS = 20;
    private static final long GARBAGE_COLLECTION_PAUSE_MS = 50L;

    private TopologyTestDriver driver;

    /** Cached stores whose value is far smaller than the raw input it is derived from. */
    enum CachedStoreShape {
        // CachingKeyValueStore
        MAPPED_VALUE_TABLE {
            @Override
            void build(final StreamsBuilder builder, final boolean cachingEnabled) {
                final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
                    Materialized.<String, String>as(Stores.persistentKeyValueStore("table-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String());
                rawInputStream(builder)
                    .mapValues(rawValue -> "v")
                    .toTable(cachingEnabled ? materialized.withCachingEnabled() : materialized.withCachingDisabled());
            }
        },
        // CachingKeyValueStore, via an aggregation
        COUNT {
            @Override
            void build(final StreamsBuilder builder, final boolean cachingEnabled) {
                final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized =
                    Materialized.as(Stores.persistentKeyValueStore("count-store"));
                grouped(builder)
                    .count(cachingEnabled ? materialized.withCachingEnabled() : materialized.withCachingDisabled());
            }
        },
        // CachingWindowStore
        WINDOWED_COUNT {
            @Override
            void build(final StreamsBuilder builder, final boolean cachingEnabled) {
                final Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized =
                    Materialized.as(Stores.persistentWindowStore("windowed-count-store", WINDOW, WINDOW, false));
                grouped(builder)
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW))
                    .count(cachingEnabled ? materialized.withCachingEnabled() : materialized.withCachingDisabled());
            }
        },
        // CachingSessionStore
        SESSION_WINDOWED_COUNT {
            @Override
            void build(final StreamsBuilder builder, final boolean cachingEnabled) {
                final Materialized<String, Long, SessionStore<Bytes, byte[]>> materialized =
                    Materialized.as(Stores.persistentSessionStore("session-count-store", WINDOW));
                grouped(builder)
                    .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(WINDOW))
                    .count(cachingEnabled ? materialized.withCachingEnabled() : materialized.withCachingDisabled());
            }
        },

        // InMemoryTimeOrderedKeyValueChangeBuffer, bounded by the same budget as the cache
        SUPPRESSED_TABLE {
            @Override
            void build(final StreamsBuilder builder, final boolean cachingEnabled) {
                final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
                    Materialized.<String, String>as(Stores.persistentKeyValueStore("suppressed-table-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled();
                rawInputStream(builder)
                    .mapValues(rawValue -> "v")
                    .toTable(materialized)
                    .suppress(Suppressed.untilTimeLimit(
                        Duration.ofHours(1),
                        Suppressed.BufferConfig.maxBytes(CACHE_MAX_BYTES).emitEarlyWhenFull()));
            }
        };

        abstract void build(StreamsBuilder builder, boolean cachingEnabled);

        private static KStream<String, byte[]> rawInputStream(final StreamsBuilder builder) {
            return builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.ByteArray()));
        }

        private static KGroupedStream<String, byte[]> grouped(final StreamsBuilder builder) {
            return rawInputStream(builder).groupByKey(Grouped.with(Serdes.String(), Serdes.ByteArray()));
        }
    }

    @AfterEach
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @ParameterizedTest
    @EnumSource(CachedStoreShape.class)
    public void shouldNotRetainSourceRawRecordsInCacheWithoutDeadLetterQueue(final CachedStoreShape shape) {
        final RawValueTrackingSerializer rawValues = processDistinctKeys(shape, true, streamsConfig(null));

        assertRawInputExceedsCacheBudget(rawValues);
        // only the record processed last may still be reachable, as with caching disabled
        final long retainedBytes = rawValues.reachableBytesAfterGarbageCollection();
        assertTrue(retainedBytes <= RAW_VALUE_SIZE_BYTES,
            shape + " without a dead-letter queue retained " + retainedBytes + " raw bytes; expected at most one record ("
                + RAW_VALUE_SIZE_BYTES + ")");
    }

    @ParameterizedTest
    @EnumSource(CachedStoreShape.class)
    public void shouldBoundRetainedSourceRawRecordsByCacheSizeWithDeadLetterQueue(final CachedStoreShape shape) {
        final RawValueTrackingSerializer rawValues =
            processDistinctKeys(shape, true, streamsConfig(DEAD_LETTER_QUEUE_TOPIC));

        assertRawInputExceedsCacheBudget(rawValues);
        // the raw records are kept for the dead-letter queue, so they count against the cache budget
        final long allowedBytes = (long) (CACHE_MAX_BYTES * CACHE_OVERHEAD_FACTOR) + RAW_VALUE_SIZE_BYTES;
        final long retainedBytes = rawValues.reachableBytesAfterGarbageCollection();
        assertTrue(retainedBytes <= allowedBytes,
            shape + " with a dead-letter queue retained " + retainedBytes + " raw bytes; statestore.cache.max.bytes is "
                + CACHE_MAX_BYTES + ", allowed " + allowedBytes);
    }

    @Test
    public void shouldNotRetainSourceRawRecordsWithCachingDisabled() {
        // with no cache, only the record processed last may still be reachable
        final RawValueTrackingSerializer rawValues =
            processDistinctKeys(CachedStoreShape.MAPPED_VALUE_TABLE, false, streamsConfig(DEAD_LETTER_QUEUE_TOPIC));

        final long retainedBytes = rawValues.reachableBytesAfterGarbageCollection();
        assertTrue(retainedBytes <= RAW_VALUE_SIZE_BYTES,
            "uncached store retained " + retainedBytes + " raw bytes; expected at most one record");
    }

    private RawValueTrackingSerializer processDistinctKeys(final CachedStoreShape shape,
                                                           final boolean cachingEnabled,
                                                           final Properties config) {
        final StreamsBuilder builder = new StreamsBuilder();
        shape.build(builder, cachingEnabled);

        driver = new TopologyTestDriver(builder.build(), config);
        final RawValueTrackingSerializer rawValues = new RawValueTrackingSerializer();
        final TestInputTopic<String, String> input =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), rawValues);
        for (int key = 0; key < DISTINCT_KEYS; key++) {
            input.pipeInput("key-" + key, "value");
        }
        return rawValues;
    }

    private static Properties streamsConfig(final String deadLetterQueueTopic) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "record-cache-source-raw-record-memory-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, CACHE_MAX_BYTES);
        if (deadLetterQueueTopic != null) {
            config.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, deadLetterQueueTopic);
        }
        return config;
    }

    private static void assertRawInputExceedsCacheBudget(final RawValueTrackingSerializer rawValues) {
        assertTrue(rawValues.trackedBytes() > 10 * CACHE_MAX_BYTES,
            "raw input (" + rawValues.trackedBytes() + " bytes) must be many times the cache budget for the test to mean anything");
    }

    /**
     * Returns a fresh {@code RAW_VALUE_SIZE_BYTES} array per record and keeps a weak reference to each.
     * The test driver passes that same array to Streams as the record's raw value.
     */
    private static final class RawValueTrackingSerializer implements Serializer<String> {
        private final List<WeakReference<byte[]>> tracked = new ArrayList<>();

        @Override
        public byte[] serialize(final String topic, final String data) {
            final byte[] rawValue = new byte[RAW_VALUE_SIZE_BYTES];
            tracked.add(new WeakReference<>(rawValue));
            return rawValue;
        }

        long trackedBytes() {
            return (long) tracked.size() * RAW_VALUE_SIZE_BYTES;
        }

        /** Collects garbage until at most one record is left or the attempts run out, then sums what survives. */
        long reachableBytesAfterGarbageCollection() {
            for (int attempt = 0; attempt < GARBAGE_COLLECTION_ATTEMPTS && reachableBytes() > RAW_VALUE_SIZE_BYTES; attempt++) {
                System.gc();
                try {
                    Thread.sleep(GARBAGE_COLLECTION_PAUSE_MS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return reachableBytes();
        }

        private long reachableBytes() {
            return tracked.stream()
                .map(WeakReference::get)
                .filter(Objects::nonNull)
                .mapToLong(rawValue -> rawValue.length)
                .sum();
        }
    }
}
