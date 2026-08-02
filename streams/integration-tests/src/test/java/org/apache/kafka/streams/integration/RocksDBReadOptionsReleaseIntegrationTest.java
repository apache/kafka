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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code RocksDBStore} releases {@code RocksDB.defaultReadOptions_} after {@code db.close()}, which
 * is safe only because the database is already closed and every open iterator was closed first.
 * That ordering is the whole safety argument, so it is worth exercising against a real broker with
 * segments rolling and expiring underneath live iterators.
 *
 * <p>Record timestamps advance a minute apart, so a short retention churns segments continuously
 * while interactive queries iterate across them. A use-after-free would surface here as a crash, a
 * corrupted read, or a processing exception.
 */
@Tag("integration")
@Timeout(600)
public class RocksDBReadOptionsReleaseIntegrationTest {

    private static final int NUM_BROKERS = 1;

    private static final String STORE_NAME = "windowed-counts";
    private static final long WINDOW_SIZE_MS = 10_000L;
    /** Segment interval is derived as max(retention / 2, 60s), so this yields 2.5 min segments. */
    private static final long RETENTION_MS = 300_000L;
    /** One minute of stream time per record: rolls a segment every few records. */
    private static final long ADVANCE_MS = 60_000L;
    private static final int NUM_RECORDS = 90;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private KafkaStreams kafkaStreams;
    private Properties streamsConfiguration;
    private String inputTopic;
    private String outputTopic;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        final String safeTestName = safeUniqueTestName(testInfo);
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(inputTopic, 2, 1);
        CLUSTER.createTopic(outputTopic, 2, 1);

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        // No cache, so every record reaches the store and forces segment work.
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    @AfterEach
    public void after() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
            kafkaStreams.cleanUp();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldServeQueriesCorrectlyWhileSegmentsRollAndExpire() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_SIZE_MS)))
                .count(Materialized.<String, Long>as(
                        Stores.persistentWindowStore(
                                STORE_NAME,
                                Duration.ofMillis(RETENTION_MS),
                                Duration.ofMillis(WINDOW_SIZE_MS),
                                false)))
                .toStream()
                .map((windowedKey, count) ->
                        KeyValue.pair(windowedKey.key() + "@" + windowedKey.window().start(), count))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final AtomicReference<Throwable> uncaught = new AtomicReference<>();
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.setUncaughtExceptionHandler(throwable -> {
            uncaught.compareAndSet(null, throwable);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        // Hammer the store with iterators while segments roll and expire underneath.
        final CountDownLatch stopQuerying = new CountDownLatch(1);
        final AtomicInteger successfulQueries = new AtomicInteger();
        final AtomicInteger concurrentModifications = new AtomicInteger();
        final AtomicReference<Throwable> queryFailure = new AtomicReference<>();
        final Thread querier = new Thread(() -> {
            while (stopQuerying.getCount() > 0) {
                try {
                    final ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
                            StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
                    try (KeyValueIterator<?, ?> all =
                                 store.fetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE / 2))) {
                        while (all.hasNext()) {
                            all.next();
                        }
                    }
                    try (WindowStoreIterator<Long> single =
                                 store.fetch("k0", Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE / 2))) {
                        while (single.hasNext()) {
                            single.next();
                        }
                    }
                    successfulQueries.incrementAndGet();
                } catch (final InvalidStateStoreException rebalancing) {
                    // Expected while the store migrates; keep going.
                } catch (final ConcurrentModificationException preExistingRace) {
                    // NOT caused by releasing defaultReadOptions_. AbstractSegments holds its
                    // segments in a plain TreeMap, so an interactive query iterating
                    // AbstractSegments.segments() races the StreamThread creating or expiring a
                    // segment. Verified to reproduce at the same rate with the release reverted, so
                    // it is counted and tolerated rather than allowed to mask what this test covers.
                    concurrentModifications.incrementAndGet();
                } catch (final Throwable t) {
                    queryFailure.compareAndSet(null, t);
                    return;
                }
            }
        }, "iterator-stress");
        querier.setDaemon(true);
        querier.start();

        produceAdvancingRecords();

        final List<KeyValue<String, Long>> received =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(), outputTopic, NUM_RECORDS, 120_000L);

        stopQuerying.countDown();
        querier.join(Duration.ofSeconds(30).toMillis());

        if (queryFailure.get() != null) {
            throw new AssertionError(
                    "interactive queries must not fail while segments expire", queryFailure.get());
        }
        if (uncaught.get() != null) {
            throw new AssertionError("the topology must not raise an uncaught exception", uncaught.get());
        }
        assertTrue(successfulQueries.get() > 0,
                "expected at least one successful query against a live store, got " + successfulQueries.get());

        // Each record sits in its own window, so every record yields exactly one aggregate update.
        assertEquals(NUM_RECORDS, received.size(),
                "every record should produce one windowed count despite segment expiry");
        for (final KeyValue<String, Long> kv : received) {
            assertEquals(1L, kv.value, "each window holds exactly one record: " + kv.key);
        }
    }

    private void produceAdvancingRecords() {
        final Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", CLUSTER.bootstrapServers());
        producerConfig.put("key.serializer", StringSerializer.class);
        producerConfig.put("value.serializer", StringSerializer.class);

        final List<KeyValue<String, String>> batch = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            batch.add(KeyValue.pair("k" + (i % 4), "v" + i));
        }
        long timestamp = 0L;
        for (final KeyValue<String, String> record : batch) {
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                    inputTopic, List.of(record), producerConfig, timestamp);
            timestamp += ADVANCE_MS;
        }
    }

    private Properties consumerConfig() {
        final Properties config = new Properties();
        config.put("bootstrap.servers", CLUSTER.bootstrapServers());
        config.put("group.id", "verifier-" + System.nanoTime());
        config.put("auto.offset.reset", "earliest");
        config.put("key.deserializer", StringDeserializer.class);
        config.put("value.deserializer", LongDeserializer.class);
        return config;
    }
}
