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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.CloseOptions.GroupMembershipOperation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStateRestoreListener;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getRunningStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.sessionStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.apache.kafka.test.StreamsTestUtils.toListAndCloseIterator;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests that verify IQv1 works with the headers-aware state stores of KIP-1271 and KIP-1285.
 *
 * <p>The DSL tests create the stores either from a {@code Stores.*WithHeaders} supplier or from
 * {@code dsl.store.format=HEADERS}. DSL operators write empty headers to their stores (see the KIP-1285
 * current limitations), so these tests produce records with headers and assert that the queried headers are
 * empty. The Processor API tests write the record headers into the stores, and assert that IQv1 returns them.
 *
 * <p>IQv2 on headers-aware stores is covered by {@link IQv2HeadersStoreIntegrationTest}.
 */
@Timeout(600)
@Tag("integration")
public class IQv1HeadersStoreIntegrationTest {
    private static final long DEFAULT_TIMEOUT_MS = 120 * 1000;
    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 2;

    // stores of the Processor API topology built by topology()
    private static final String HEADERS_KV_STORE = "headers-kv-store";
    private static final String HEADERS_WINDOW_STORE = "headers-window-store";
    private static final String HEADERS_SESSION_STORE = "headers-session-store";
    private static final long HEADERS_WINDOW_SIZE_MS = 10_000L;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldQueryHeadersAwareKeyValueStore(final boolean viaDslConfig, final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            // without caching, approximateNumEntries() counts every record as soon as it is processed
            keyValueMaterialized(storeName, viaDslConfig).withCachingDisabled()
        );

        try (final KafkaStreams streams = getRunningStreams(streamsProperties(uniqueTestName, viaDslConfig), builder, true)) {
            final List<KeyValue<String, String>> records = Arrays.asList(
                new KeyValue<>("alpha", "a1"),
                new KeyValue<>("beta", "b1"),
                new KeyValue<>("gamma", "g1"),
                new KeyValue<>("delta", "d1"),
                new KeyValue<>("epsilon", "e1")
            );
            final long timestamp = System.currentTimeMillis();
            produceSynchronously(records.stream()
                .map(record -> new ProducerRecord<>(inputTopic, null, timestamp, record.key, record.value, dslInputHeaders()))
                .collect(Collectors.toList()));

            // sorted by key: "alpha", "beta", "delta", "epsilon", "gamma"
            final List<KeyValue<String, String>> sortedRecords = records.stream()
                .sorted(Comparator.comparing(record -> record.key))
                .collect(Collectors.toList());
            final List<KeyValue<String, ValueTimestampHeaders<String>>> entries = sortedRecords.stream()
                .map(record -> KeyValue.pair(record.key, ValueTimestampHeaders.make(record.value, timestamp, new RecordHeaders())))
                .collect(Collectors.toList());

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(entries.get(0).value, store.get("alpha"));
                assertNull(store.get("nonexistent"));
                // exact, since all keys are distinct and none was deleted
                assertEquals(5L, store.approximateNumEntries());

                assertEquals(entries, toListAndCloseIterator(store.all()));
                assertEquals(reversed(entries), toListAndCloseIterator(store.reverseAll()));
                assertEquals(entries.subList(1, 4), toListAndCloseIterator(store.range("beta", "epsilon")));
                assertEquals(reversed(entries.subList(1, 4)), toListAndCloseIterator(store.reverseRange("beta", "epsilon")));
                assertEquals(entries.subList(2, 3), toListAndCloseIterator(store.prefixScan("de", new StringSerializer())));

                final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> tsStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStore()));
                assertEquals(ValueAndTimestamp.make("b1", timestamp), tsStore.get("beta"));

                final ReadOnlyKeyValueStore<String, String> plainStore = streams.store(fromNameAndType(storeName, keyValueStore()));
                assertEquals("b1", plainStore.get("beta"));
                assertEquals(sortedRecords, toListAndCloseIterator(plainStore.all()));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldQueryHeadersAwareGlobalStore(final boolean viaDslConfig, final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-global-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            keyValueMaterialized(storeName, viaDslConfig)
        );

        final List<KeyValue<String, String>> records = Arrays.asList(
            new KeyValue<>("alpha", "a1"),
            new KeyValue<>("beta", "b1")
        );
        final long timestamp = System.currentTimeMillis();
        final Properties properties = streamsProperties(uniqueTestName, viaDslConfig);

        try (final KafkaStreams streams = getRunningStreams(properties, builder, true)) {
            produceSynchronously(records.stream()
                .map(record -> new ProducerRecord<>(inputTopic, null, timestamp, record.key, record.value, dslInputHeaders()))
                .collect(Collectors.toList()));

            // the global table processor writes empty headers to the store
            retryOnExceptionWithTimeout(30_000, () -> verifyGlobalTable(streams, storeName, records, timestamp, new RecordHeaders()));
        }

        // With an empty state directory, the store is restored from its source topic. Unlike addGlobalStore(),
        // globalTable() does not reprocess the records on restore, but copies them, together with their headers,
        // into the store (see RecordConverters.rawValueToHeadersValue()). So, unlike the processor, the restore
        // keeps the headers of the source records. This pins down the current behavior.
        // TODO: update the expected headers once we address https://issues.apache.org/jira/browse/KAFKA-21493
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), properties)) {
            streams.setGlobalStateRestoreListener(restoreListener);
            startApplicationAndWaitUntilRunning(streams);

            assertEquals(records.size(), restoreListener.totalNumRestored());
            retryOnExceptionWithTimeout(30_000, () -> verifyGlobalTable(streams, storeName, records, timestamp, dslInputHeaders()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldQueryHeadersAwareWindowStoreAcrossMultipleAndOverlappingWindows(final boolean viaDslConfig,
                                                                                    final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-window-store";
        final long windowSize = 10_000L;
        final long advance = 5_000L;

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            // hopping windows, so that every record is counted in two overlapping windows
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(windowSize)).advanceBy(ofMillis(advance)))
            .count(windowMaterialized(storeName, windowSize, viaDslConfig));

        try (final KafkaStreams streams = getRunningStreams(streamsProperties(uniqueTestName, viaDslConfig), builder, true)) {
            // aligned to the advance, so that a window starts at t
            final long t = System.currentTimeMillis() / advance * advance;
            produceSynchronously(Arrays.asList(
                new ProducerRecord<>(inputTopic, null, t, "wa", "v1", dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + advance, "wa", "v2", dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + 4 * advance, "wb", "v3", dslInputHeaders())
            ));

            final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> windows = Arrays.asList(
                windowedCount("wa", t - advance, t - advance + windowSize, 1L, t),
                windowedCount("wa", t, t + windowSize, 2L, t + advance),
                windowedCount("wa", t + advance, t + advance + windowSize, 1L, t + advance),
                windowedCount("wb", t + 3 * advance, t + 3 * advance + windowSize, 1L, t + 4 * advance),
                windowedCount("wb", t + 4 * advance, t + 4 * advance + windowSize, 1L, t + 4 * advance)
            );
            final List<KeyValue<Long, ValueTimestampHeaders<Long>>> waWindows = windows.subList(0, 3).stream()
                .map(window -> KeyValue.pair(window.key.window().start(), window.value))
                .collect(Collectors.toList());
            final Instant timeFrom = ofEpochMilli(t - advance);
            final Instant timeTo = ofEpochMilli(t + 4 * advance);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyWindowStore<String, ValueTimestampHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedWindowStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(waWindows, toListAndCloseIterator(store.fetch("wa", timeFrom, timeTo)));
                assertEquals(reversed(waWindows), toListAndCloseIterator(store.backwardFetch("wa", timeFrom, timeTo)));
                // the time range bounds the start time of the windows
                assertEquals(waWindows.subList(1, 2), toListAndCloseIterator(store.fetch("wa", ofEpochMilli(t), ofEpochMilli(t))));
                assertEquals(waWindows.get(1).value, store.fetch("wa", t));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> fetchedAll =
                    toListAndCloseIterator(store.fetchAll(timeFrom, timeTo));
                assertSameEntries(windows, fetchedAll);
                assertEquals(reversed(fetchedAll), toListAndCloseIterator(store.backwardFetchAll(timeFrom, timeTo)));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> all = toListAndCloseIterator(store.all());
                assertSameEntries(windows, all);
                assertEquals(reversed(all), toListAndCloseIterator(store.backwardAll()));

                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<Long>>> keyRange =
                    toListAndCloseIterator(store.fetch("wa", "wb", timeFrom, timeTo));
                assertSameEntries(windows, keyRange);
                assertEquals(reversed(keyRange), toListAndCloseIterator(store.backwardFetch("wa", "wb", timeFrom, timeTo)));

                final ReadOnlyWindowStore<String, ValueAndTimestamp<Long>> tsStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedWindowStore()));
                assertEquals(
                    waWindows.stream()
                        .map(window -> KeyValue.pair(window.key, ValueAndTimestamp.make(window.value.value(), window.value.timestamp())))
                        .collect(Collectors.toList()),
                    toListAndCloseIterator(tsStore.fetch("wa", timeFrom, timeTo)));

                final ReadOnlyWindowStore<String, Long> plainStore =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
                assertEquals(
                    waWindows.stream().map(window -> KeyValue.pair(window.key, window.value.value())).collect(Collectors.toList()),
                    toListAndCloseIterator(plainStore.fetch("wa", timeFrom, timeTo)));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldQueryHeadersAwareSessionStoreWithMultipleAndMergedSessions(final boolean viaDslConfig,
                                                                               final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-session-store";
        final long inactivityGap = 5_000L;

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapAndGrace(ofMillis(inactivityGap), ofMinutes(1)))
            .count(sessionMaterialized(storeName, viaDslConfig));

        try (final KafkaStreams streams = getRunningStreams(streamsProperties(uniqueTestName, viaDslConfig), builder, true)) {
            final long t = System.currentTimeMillis();
            // the records of "sa" are further apart than the inactivity gap, so they are in two sessions
            produceSynchronously(Arrays.asList(
                new ProducerRecord<>(inputTopic, null, t, "sa", "v1", dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t, "sb", "v2", dslInputHeaders()),
                new ProducerRecord<>(inputTopic, null, t + 2 * inactivityGap, "sa", "v3", dslInputHeaders())
            ));

            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> firstSession = sessionCount("sa", t, t, 1L);
            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> secondSession =
                sessionCount("sa", t + 2 * inactivityGap, t + 2 * inactivityGap, 1L);
            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> otherKeySession = sessionCount("sb", t, t, 1L);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlySessionStore<String, AggregationWithHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.sessionStoreWithHeaders()));
                assertNotNull(store);

                assertEquals(Arrays.asList(firstSession, secondSession), toListAndCloseIterator(store.fetch("sa")));
                assertEquals(Arrays.asList(secondSession, firstSession), toListAndCloseIterator(store.backwardFetch("sa")));
                // only the second session ends after t
                assertEquals(
                    Collections.singletonList(secondSession),
                    toListAndCloseIterator(store.findSessions("sa", t + 1, t + 2 * inactivityGap)));
                // only the first session starts at or before t
                assertEquals(
                    Collections.singletonList(firstSession),
                    toListAndCloseIterator(store.backwardFindSessions("sa", t, t)));

                final List<KeyValue<Windowed<String>, AggregationWithHeaders<Long>>> keyRange =
                    toListAndCloseIterator(store.fetch("sa", "sb"));
                assertSameEntries(Arrays.asList(firstSession, secondSession, otherKeySession), keyRange);
                assertEquals(reversed(keyRange), toListAndCloseIterator(store.backwardFetch("sa", "sb")));

                final ReadOnlySessionStore<String, Long> plainStore = streams.store(fromNameAndType(storeName, sessionStore()));
                assertEquals(
                    Arrays.asList(KeyValue.pair(firstSession.key, 1L), KeyValue.pair(secondSession.key, 1L)),
                    toListAndCloseIterator(plainStore.fetch("sa")));
            });

            // a record within the inactivity gap of both sessions merges them into one
            produceSynchronously(Collections.singletonList(
                new ProducerRecord<>(inputTopic, null, t + inactivityGap, "sa", "v4", dslInputHeaders())));

            final KeyValue<Windowed<String>, AggregationWithHeaders<Long>> mergedSession =
                sessionCount("sa", t, t + 2 * inactivityGap, 3L);

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlySessionStore<String, AggregationWithHeaders<Long>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.sessionStoreWithHeaders()));

                assertEquals(Collections.singletonList(mergedSession), toListAndCloseIterator(store.fetch("sa")));
                assertEquals(Collections.singletonList(mergedSession), toListAndCloseIterator(store.findSessions("sa", t, t)));
            });
        }
    }

    @Test
    public void shouldQueryHeadersAwareKeyValueStoreAfterUpdateAndTombstone(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-store";

        CLUSTER.createTopic(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            keyValueMaterialized(storeName, false)
        );

        try (final KafkaStreams streams = getRunningStreams(streamsProperties(uniqueTestName, false), builder, true)) {
            final long t = System.currentTimeMillis();
            produceSynchronously(Collections.singletonList(
                new ProducerRecord<>(inputTopic, null, t, "key", "v1", dslInputHeaders())));

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertEquals(ValueTimestampHeaders.make("v1", t, new RecordHeaders()), store.get("key"));
            });

            produceSynchronously(Collections.singletonList(
                new ProducerRecord<>(inputTopic, null, t + 1, "key", "v2", dslInputHeaders())));

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                final ValueTimestampHeaders<String> expected = ValueTimestampHeaders.make("v2", t + 1, new RecordHeaders());
                assertEquals(expected, store.get("key"));
                assertEquals(Collections.singletonList(KeyValue.pair("key", expected)), toListAndCloseIterator(store.all()));
            });

            produceSynchronously(Collections.singletonList(
                new ProducerRecord<>(inputTopic, null, t + 2, "key", null, dslInputHeaders())));

            retryOnExceptionWithTimeout(30_000, () -> {
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
                    streams.store(fromNameAndType(storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders()));
                assertNull(store.get("key"));
                assertEquals(Collections.emptyList(), toListAndCloseIterator(store.all()));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldReturnRecordHeadersFromHeadersAwareStores(final boolean cachingEnabled, final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        try (final KafkaStreams streams = getRunningStreams(
                streamsProperties(uniqueTestName, false), topology(inputTopic, cachingEnabled), true)) {
            final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
            produceSynchronously(records);

            // sorted by key: "duplicate", "multi", "none", "single"
            final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = latestValuesByKey(records);
            final ProducerRecord<String, String> firstRecord = records.get(0);
            final ProducerRecord<String, String> lastRecord = records.get(records.size() - 1);
            final Instant timeFrom = ofEpochMilli(windowStart(firstRecord.timestamp()));
            final Instant timeTo = ofEpochMilli(windowStart(lastRecord.timestamp()));

            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    verifyForKey(streams, null, false, latestValue.key, records);
                }

                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> kvStore =
                    store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false);
                assertEquals(latestValues, toListAndCloseIterator(kvStore.all()));
                assertEquals(reversed(latestValues), toListAndCloseIterator(kvStore.reverseAll()));
                assertEquals(latestValues.subList(1, 3), toListAndCloseIterator(kvStore.range("multi", "none")));
                assertEquals(reversed(latestValues.subList(1, 3)), toListAndCloseIterator(kvStore.reverseRange("multi", "none")));
                assertEquals(latestValues.subList(3, 4), toListAndCloseIterator(kvStore.prefixScan("si", new StringSerializer())));

                final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> windowStore =
                    store(streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> windows =
                    records.stream().map(IQv1HeadersStoreIntegrationTest::window).collect(Collectors.toList());
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> allWindows =
                    toListAndCloseIterator(windowStore.all());
                assertSameEntries(windows, allWindows);
                assertEquals(reversed(allWindows), toListAndCloseIterator(windowStore.backwardAll()));
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> fetchedWindows =
                    toListAndCloseIterator(windowStore.fetchAll(timeFrom, timeTo));
                assertSameEntries(windows, fetchedWindows);
                assertEquals(reversed(fetchedWindows), toListAndCloseIterator(windowStore.backwardFetchAll(timeFrom, timeTo)));
                // only the last record is in the later window
                assertEquals(
                    Collections.singletonList(window(lastRecord)),
                    toListAndCloseIterator(windowStore.fetch("duplicate", "single", timeTo, timeTo)));
                assertEquals(valueTimestampHeaders(firstRecord), windowStore.fetch("multi", timeFrom.toEpochMilli()));
                // the two records of "multi" are in two windows
                final List<KeyValue<Long, ValueTimestampHeaders<String>>> multiWindows = Arrays.asList(
                    KeyValue.pair(timeFrom.toEpochMilli(), valueTimestampHeaders(firstRecord)),
                    KeyValue.pair(timeTo.toEpochMilli(), valueTimestampHeaders(lastRecord)));
                assertEquals(reversed(multiWindows), toListAndCloseIterator(windowStore.backwardFetch("multi", timeFrom, timeTo)));

                final ReadOnlySessionStore<String, AggregationWithHeaders<String>> sessionStore =
                    store(streams, HEADERS_SESSION_STORE, QueryableStoreTypes.sessionStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> sessions =
                    records.stream().map(IQv1HeadersStoreIntegrationTest::session).collect(Collectors.toList());
                final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> fetchedSessions =
                    toListAndCloseIterator(sessionStore.fetch("duplicate", "single"));
                assertSameEntries(sessions, fetchedSessions);
                assertEquals(reversed(fetchedSessions), toListAndCloseIterator(sessionStore.backwardFetch("duplicate", "single")));
                // null bounds fetch the sessions of all keys
                final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> allSessions =
                    toListAndCloseIterator(sessionStore.fetch(null, null));
                assertSameEntries(sessions, allSessions);
                assertEquals(reversed(allSessions), toListAndCloseIterator(sessionStore.backwardFetch(null, null)));
                // only the session of the last record ends after the first record
                assertEquals(
                    Collections.singletonList(session(lastRecord)),
                    toListAndCloseIterator(sessionStore.findSessions("multi", firstRecord.timestamp() + 1, lastRecord.timestamp())));
                assertEquals(
                    Collections.singletonList(session(firstRecord)),
                    toListAndCloseIterator(sessionStore.backwardFindSessions("multi", firstRecord.timestamp(), firstRecord.timestamp())));
                assertEquals(
                    session(firstRecord).value,
                    sessionStore.fetchSession("multi", firstRecord.timestamp(), firstRecord.timestamp()));

                // the timestamped and plain query types return the same records without their headers
                final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> tsKvStore =
                    store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStore(), null, false);
                assertEquals(
                    latestValues.stream()
                        .map(entry -> KeyValue.pair(entry.key, ValueAndTimestamp.make(entry.value.value(), entry.value.timestamp())))
                        .collect(Collectors.toList()),
                    toListAndCloseIterator(tsKvStore.all()));
                final ReadOnlyKeyValueStore<String, String> plainKvStore = store(streams, HEADERS_KV_STORE, keyValueStore(), null, false);
                assertEquals(
                    latestValues.stream().map(entry -> KeyValue.pair(entry.key, entry.value.value())).collect(Collectors.toList()),
                    toListAndCloseIterator(plainKvStore.all()));

                final ReadOnlyWindowStore<String, ValueAndTimestamp<String>> tsWindowStore =
                    store(streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStore(), null, false);
                assertSameEntries(
                    windows.stream()
                        .map(window -> KeyValue.pair(window.key, ValueAndTimestamp.make(window.value.value(), window.value.timestamp())))
                        .collect(Collectors.toList()),
                    toListAndCloseIterator(tsWindowStore.fetchAll(timeFrom, timeTo)));
                final ReadOnlyWindowStore<String, String> plainWindowStore =
                    store(streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.windowStore(), null, false);
                assertEquals(
                    multiWindows.stream().map(window -> KeyValue.pair(window.key, window.value.value())).collect(Collectors.toList()),
                    toListAndCloseIterator(plainWindowStore.fetch("multi", timeFrom, timeTo)));

                final ReadOnlySessionStore<String, String> plainSessionStore =
                    store(streams, HEADERS_SESSION_STORE, sessionStore(), null, false);
                assertSameEntries(
                    sessions.stream().map(session -> KeyValue.pair(session.key, session.value.aggregation())).collect(Collectors.toList()),
                    toListAndCloseIterator(plainSessionStore.fetch("duplicate", "single")));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldHandleNullKeysAndNullBoundsOfHeadersAwareStores(final boolean cachingEnabled,
                                                                       final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        try (final KafkaStreams streams = getRunningStreams(
                streamsProperties(uniqueTestName, false), topology(inputTopic, cachingEnabled), true)) {
            final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
            produceSynchronously(records);

            // sorted by key: "duplicate", "multi", "none", "single"
            final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = latestValuesByKey(records);
            final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> windows =
                records.stream().map(IQv1HeadersStoreIntegrationTest::window).collect(Collectors.toList());
            final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> sessions =
                records.stream().map(IQv1HeadersStoreIntegrationTest::session).collect(Collectors.toList());
            final long firstTimestamp = records.get(0).timestamp();
            final long lastTimestamp = records.get(records.size() - 1).timestamp();
            final Instant timeFrom = ofEpochMilli(windowStart(firstTimestamp));
            final Instant timeTo = ofEpochMilli(windowStart(lastTimestamp));

            retryOnExceptionWithTimeout(30_000, () -> {
                // a null key is rejected by the store, while a null bound of a range means that the range is unbounded
                final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> kvStore =
                    store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false);
                assertEquals(latestValues, toListAndCloseIterator(kvStore.range(null, null)));
                assertEquals(reversed(latestValues), toListAndCloseIterator(kvStore.reverseRange(null, null)));
                assertEquals(latestValues.subList(0, 2), toListAndCloseIterator(kvStore.range(null, "multi")));
                assertEquals(reversed(latestValues.subList(2, 4)), toListAndCloseIterator(kvStore.reverseRange("none", null)));
                assertThrows(NullPointerException.class, () -> kvStore.get(null));

                final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> windowStore =
                    store(streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, ValueTimestampHeaders<String>>> fetchedWindows =
                    toListAndCloseIterator(windowStore.fetch(null, null, timeFrom, timeTo));
                assertSameEntries(windows, fetchedWindows);
                assertEquals(reversed(fetchedWindows), toListAndCloseIterator(windowStore.backwardFetch(null, null, timeFrom, timeTo)));
                assertSameEntries(
                    windows.stream().filter(window -> window.key.key().compareTo("multi") <= 0).collect(Collectors.toList()),
                    toListAndCloseIterator(windowStore.fetch(null, "multi", timeFrom, timeTo)));
                assertThrows(NullPointerException.class, () -> windowStore.fetch(null, timeFrom.toEpochMilli()));
                assertThrows(NullPointerException.class, () -> windowStore.fetch(null, timeFrom, timeTo));
                assertThrows(NullPointerException.class, () -> windowStore.backwardFetch(null, timeFrom, timeTo));

                final ReadOnlySessionStore<String, AggregationWithHeaders<String>> sessionStore =
                    store(streams, HEADERS_SESSION_STORE, QueryableStoreTypes.sessionStoreWithHeaders(), null, false);
                final List<KeyValue<Windowed<String>, AggregationWithHeaders<String>>> foundSessions =
                    toListAndCloseIterator(sessionStore.findSessions(null, null, firstTimestamp, lastTimestamp));
                assertSameEntries(sessions, foundSessions);
                assertEquals(
                    reversed(foundSessions),
                    toListAndCloseIterator(sessionStore.backwardFindSessions(null, null, firstTimestamp, lastTimestamp)));
                assertSameEntries(
                    sessions.stream().filter(session -> session.key.key().compareTo("none") >= 0).collect(Collectors.toList()),
                    toListAndCloseIterator(sessionStore.fetch("none", null)));
                assertThrows(NullPointerException.class, () -> sessionStore.fetch(null));
                assertThrows(NullPointerException.class, () -> sessionStore.backwardFetch(null));
                assertThrows(NullPointerException.class, () -> sessionStore.findSessions(null, firstTimestamp, lastTimestamp));
                assertThrows(NullPointerException.class, () -> sessionStore.fetchSession(null, firstTimestamp, firstTimestamp));
            });
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromHeadersAwareGlobalStoreBeforeAndAfterRestoration(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";
        final String storeName = uniqueTestName + "-global-store";

        CLUSTER.createTopic(inputTopic);

        final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
        final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = latestValuesByKey(records);
        final Properties properties = streamsProperties(uniqueTestName, false);

        // the global store is updated by the processor while the application runs
        try (final KafkaStreams streams = getRunningStreams(properties, globalTopology(inputTopic, storeName), true)) {
            produceSynchronously(records);
            retryOnExceptionWithTimeout(30_000, () -> verifyGlobalStore(streams, storeName, latestValues));
        }

        // With an empty state directory, the global store is restored from its source topic. addGlobalStore()
        // reprocesses the restored records through GlobalWriter, so the processor sees their headers again.
        // The restore that copies the records into the store, without a processor, is covered by
        // shouldQueryHeadersAwareGlobalStore().
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        try (final KafkaStreams streams = new KafkaStreams(globalTopology(inputTopic, storeName).build(), properties)) {
            streams.setGlobalStateRestoreListener(restoreListener);
            startApplicationAndWaitUntilRunning(streams);

            assertEquals(records.size(), restoreListener.totalNumRestored());
            retryOnExceptionWithTimeout(30_000, () -> verifyGlobalStore(streams, storeName, latestValues));
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromHeadersAwareStoresAfterRestart(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
        final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = latestValuesByKey(records);
        final Properties properties = streamsProperties(uniqueTestName, false);

        try (final KafkaStreams streams = getRunningStreams(properties, topology(inputTopic, false), true)) {
            produceSynchronously(records);
            // all records are in the stores before the restart, so the restarted instance does not reprocess any of them
            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    verifyForKey(streams, null, false, latestValue.key, records);
                }
            });
            closeAndLeaveGroup(streams);
        }

        // the state directory is kept, so the restarted instance reopens its local stores
        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic, false).build(), properties)) {
            streams.setGlobalStateRestoreListener(restoreListener);
            startApplicationAndWaitUntilRunning(streams);

            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    verifyForKey(streams, null, false, latestValue.key, records);
                }
            });
            assertEquals(0L, restoreListener.totalNumRestored());
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromActiveAndStandbyHostsBeforeAndAfterRebalance(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic, NUM_PARTITIONS, NUM_BROKERS);

        final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
        final Set<String> keys = records.stream().map(ProducerRecord::key).collect(Collectors.toSet());
        produceSynchronously(records);

        final List<KafkaStreams> streamsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final Properties properties = streamsProperties(uniqueTestName, false);
            // the port is the index of the instance in streamsList
            properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i);
            properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            streamsList.add(new KafkaStreams(topology(inputTopic, false).build(), properties));
        }

        try {
            startApplicationAndWaitUntilRunning(streamsList, ofSeconds(60));

            final StringSerializer serializer = new StringSerializer();
            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> {
                final Set<Integer> activePorts = new HashSet<>();
                for (final String key : keys) {
                    final KeyQueryMetadata metadata = streamsList.get(0).queryMetadataForKey(HEADERS_KV_STORE, key, serializer);
                    assertEquals(1, metadata.standbyHosts().size());
                    activePorts.add(metadata.activeHost().port());
                    final KafkaStreams activeHost = streamsList.get(metadata.activeHost().port());
                    final KafkaStreams standbyHost = streamsList.get(metadata.standbyHosts().iterator().next().port());

                    verifyForKey(activeHost, metadata.partition(), false, key, records);
                    // the stores of standby tasks are only queryable as stale stores
                    verifyForKey(standbyHost, metadata.partition(), true, key, records);
                }
                // the keys are in both partitions, and each instance is the active host of one of them
                assertEquals(Set.of(0, 1), activePorts);
            });

            // the remaining instance takes over the active task of the closed one
            closeAndLeaveGroup(streamsList.get(1));
            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> assertEquals(Set.of(0, 1), activePartitions(streamsList.get(0))));
            waitForApplicationState(streamsList.subList(0, 1), State.RUNNING, ofSeconds(60));

            retryOnExceptionWithTimeout(DEFAULT_TIMEOUT_MS, () -> {
                for (final String key : keys) {
                    verifyForKey(streamsList.get(0), null, false, key, records);
                }
            });
        } finally {
            for (final KafkaStreams streams : streamsList) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldReturnRecordHeadersFromHeadersAwareStoresDuringAndAfterRestoration(final TestInfo testInfo) throws Exception {
        final String uniqueTestName = safeUniqueTestName(testInfo);
        final String inputTopic = uniqueTestName + "-input";

        CLUSTER.createTopic(inputTopic);

        final List<ProducerRecord<String, String>> records = recordsWithHeaders(inputTopic);
        final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues = latestValuesByKey(records);
        final Properties properties = streamsProperties(uniqueTestName, false);

        try (final KafkaStreams streams = getRunningStreams(properties, topology(inputTopic, false), true)) {
            produceSynchronously(records);
            retryOnExceptionWithTimeout(30_000, () -> assertEquals(latestValues, toListAndCloseIterator(
                store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false).all())));
            closeAndLeaveGroup(streams);
        }

        // with an empty state directory, the stores are restored from their changelogs
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        // restore one changelog record per batch, so that restoration can be paused after the first record
        properties.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);

        final PausingRestoreListener restoreListener = new PausingRestoreListener(HEADERS_KV_STORE);
        try (final KafkaStreams streams = new KafkaStreams(topology(inputTopic, false).build(), properties)) {
            streams.setGlobalStateRestoreListener(restoreListener);
            streams.start();

            try {
                restoreListener.awaitPause();
                // The restoring task is visible to IQ, but the stream thread does not become RUNNING until all of
                // its active tasks are restored, and only the stores of a RUNNING thread are queryable as active stores.
                assertThrows(InvalidStateStoreException.class, () ->
                    store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false)
                        .get(records.get(0).key()));
                // The changelog holds the records in the order they were processed, and caching is disabled, so the
                // first restored record is the first produced one. It is queryable as a stale store.
                assertEquals(
                    Collections.singletonList(KeyValue.pair(records.get(0).key(), valueTimestampHeaders(records.get(0)))),
                    toListAndCloseIterator(
                        store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, true).all()));
            } finally {
                restoreListener.resume();
            }

            waitForApplicationState(Collections.singletonList(streams), State.RUNNING, ofSeconds(60));
            retryOnExceptionWithTimeout(30_000, () -> {
                for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
                    verifyForKey(streams, null, false, latestValue.key, records);
                }
            });
        }
    }

    /**
     * Asserts that {@code actual} holds the entries of {@code expected}, in any order.
     */
    private static <T> void assertSameEntries(final Collection<T> expected, final List<T> actual) {
        assertEquals(expected.size(), actual.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private static <T> List<T> reversed(final List<T> list) {
        final List<T> reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }

    private static void produceSynchronously(final List<ProducerRecord<String, String>> records) throws Exception {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class))) {
            for (final ProducerRecord<String, String> record : records) {
                producer.send(record).get();
            }
        }
    }

    private static Set<Integer> activePartitions(final KafkaStreams streams) {
        return streams.metadataForLocalThreads().stream()
            .flatMap(thread -> thread.activeTasks().stream())
            .map(task -> task.taskId().partition())
            .collect(Collectors.toSet());
    }

    /**
     * @return the configuration of a single-instance application with its own state directory, which uses
     *         headers-aware stores for all DSL operators if {@code dslStoreFormatHeaders} is set
     */
    private static Properties streamsProperties(final String uniqueTestName, final boolean dslStoreFormatHeaders) {
        final Properties properties = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, uniqueTestName + "-app"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
        if (dslStoreFormatHeaders) {
            properties.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        }
        return properties;
    }

    /**
     * @return a headers-aware key-value store, created from {@code dsl.store.format=HEADERS} if
     *         {@code viaDslConfig} is set, or from a headers-aware store supplier otherwise
     */
    private static Materialized<String, String, KeyValueStore<Bytes, byte[]>> keyValueMaterialized(final String storeName,
                                                                                                   final boolean viaDslConfig) {
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = viaDslConfig
            ? Materialized.as(storeName)
            : Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName));
        return materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String());
    }

    /**
     * @return a headers-aware window store with a retention of one day, see {@link #keyValueMaterialized}
     */
    private static Materialized<String, Long, WindowStore<Bytes, byte[]>> windowMaterialized(final String storeName,
                                                                                             final long windowSize,
                                                                                             final boolean viaDslConfig) {
        final Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized = viaDslConfig
            ? Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName).withRetention(ofDays(1))
            : Materialized.as(Stores.persistentTimestampedWindowStoreWithHeaders(storeName, ofDays(1), ofMillis(windowSize), false));
        return materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());
    }

    /**
     * @return a headers-aware session store with a retention of one day, see {@link #keyValueMaterialized}
     */
    private static Materialized<String, Long, SessionStore<Bytes, byte[]>> sessionMaterialized(final String storeName,
                                                                                               final boolean viaDslConfig) {
        final Materialized<String, Long, SessionStore<Bytes, byte[]>> materialized = viaDslConfig
            ? Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(storeName).withRetention(ofDays(1))
            : Materialized.as(Stores.persistentSessionStoreWithHeaders(storeName, ofDays(1)));
        return materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());
    }

    private static Headers headers(final String... keysAndValues) {
        final Headers headers = new RecordHeaders();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            headers.add(keysAndValues[i], keysAndValues[i + 1].getBytes(StandardCharsets.UTF_8));
        }
        return headers;
    }

    /**
     * Headers of the input records of the DSL tests. DSL operators write empty headers to their stores
     * (see the KIP-1285 current limitations), so these headers must not show up in query results.
     */
    private static Headers dslInputHeaders() {
        return headers("source", "dsl-input");
    }

    /**
     * Closes {@code streams} and leaves the group, so that the next instance of the application does not
     * wait for the session timeout of this one to get its tasks.
     */
    private static void closeAndLeaveGroup(final KafkaStreams streams) {
        assertTrue(streams.close(
            CloseOptions.groupMembershipOperation(GroupMembershipOperation.LEAVE_GROUP).withTimeout(ofSeconds(60))));
    }

    private static KeyValue<Windowed<String>, ValueTimestampHeaders<Long>> windowedCount(final String key,
                                                                                       final long windowStart,
                                                                                       final long windowEnd,
                                                                                       final long count,
                                                                                       final long timestamp) {
        return KeyValue.pair(
            new Windowed<>(key, new TimeWindow(windowStart, windowEnd)),
            ValueTimestampHeaders.make(count, timestamp, new RecordHeaders()));
    }

    private static KeyValue<Windowed<String>, AggregationWithHeaders<Long>> sessionCount(final String key,
                                                                                      final long sessionStart,
                                                                                      final long sessionEnd,
                                                                                      final long count) {
        return KeyValue.pair(
            new Windowed<>(key, new SessionWindow(sessionStart, sessionEnd)),
            AggregationWithHeaders.make(count, new RecordHeaders()));
    }

    /**
     * Builds a Processor API topology that writes every input record, together with its headers, into a
     * headers-aware key-value, window, and session store. Unlike the DSL, which writes empty headers to its
     * stores, this lets tests verify that IQv1 returns the headers of the stored records.
     */
    private static StreamsBuilder topology(final String inputTopic, final boolean cachingEnabled) {
        final List<StoreBuilder<?>> storeBuilders = Arrays.asList(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(HEADERS_KV_STORE),
                Serdes.String(),
                Serdes.String()),
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(HEADERS_WINDOW_STORE, ofDays(1), ofMillis(HEADERS_WINDOW_SIZE_MS), false),
                Serdes.String(),
                Serdes.String()),
            Stores.sessionStoreWithHeadersBuilder(
                Stores.persistentSessionStoreWithHeaders(HEADERS_SESSION_STORE, ofDays(1)),
                Serdes.String(),
                Serdes.String())
        );

        final StreamsBuilder builder = new StreamsBuilder();
        for (final StoreBuilder<?> storeBuilder : storeBuilders) {
            builder.addStateStore(cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled());
        }
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .process(Writer::new, HEADERS_KV_STORE, HEADERS_WINDOW_STORE, HEADERS_SESSION_STORE);
        return builder;
    }

    /**
     * Builds a Processor API topology with a headers-aware global key-value store, which {@link GlobalWriter}
     * updates with the records of {@code inputTopic} and their headers.
     */
    private static StreamsBuilder globalTopology(final String inputTopic, final String storeName) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addGlobalStore(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName),
                Serdes.String(),
                Serdes.String()).withLoggingDisabled(),
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            () -> new GlobalWriter(storeName));
        return builder;
    }

    /**
     * Verifies that the headers-aware store of a global table holds {@code records}, with {@code timestamp}
     * and {@code headers}, through the headers-aware, timestamped, and plain query types.
     */
    private static void verifyGlobalTable(final KafkaStreams streams,
                                          final String storeName,
                                          final List<KeyValue<String, String>> records,
                                          final long timestamp,
                                          final Headers headers) {
        final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
            store(streams, storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false);
        final List<KeyValue<String, ValueTimestampHeaders<String>>> entries = records.stream()
            .map(record -> KeyValue.pair(record.key, ValueTimestampHeaders.make(record.value, timestamp, headers)))
            .collect(Collectors.toList());
        assertEquals(entries, toListAndCloseIterator(store.all()));
        assertEquals(entries.get(1).value, store.get("beta"));

        final ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> tsStore =
            store(streams, storeName, QueryableStoreTypes.timestampedKeyValueStore(), null, false);
        assertEquals(ValueAndTimestamp.make("b1", timestamp), tsStore.get("beta"));

        final ReadOnlyKeyValueStore<String, String> plainStore = store(streams, storeName, keyValueStore(), null, false);
        assertEquals(records, toListAndCloseIterator(plainStore.all()));
    }

    private static void verifyGlobalStore(final KafkaStreams streams,
                                          final String storeName,
                                          final List<KeyValue<String, ValueTimestampHeaders<String>>> latestValues) {
        final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> store =
            store(streams, storeName, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), null, false);
        for (final KeyValue<String, ValueTimestampHeaders<String>> latestValue : latestValues) {
            assertEquals(latestValue.value, store.get(latestValue.key));
        }
        assertEquals(latestValues, toListAndCloseIterator(store.all()));
        assertEquals(latestValues.subList(1, 3), toListAndCloseIterator(store.range("multi", "none")));
    }

    /**
     * Records with multiple headers whose keys are not sorted, a repeated header key, a single header, and no
     * headers. Key "multi" is written twice, into two different windows and sessions.
     */
    private static List<ProducerRecord<String, String>> recordsWithHeaders(final String topic) {
        final long timestamp = System.currentTimeMillis();
        return Arrays.asList(
            new ProducerRecord<>(topic, null, timestamp, "multi", "v1", headers("z", "1", "a", "2", "m", "3")),
            new ProducerRecord<>(topic, null, timestamp, "duplicate", "v2", headers("dup", "1", "dup", "2")),
            new ProducerRecord<>(topic, null, timestamp, "single", "v3", headers("key", "value")),
            new ProducerRecord<>(topic, null, timestamp, "none", "v4", headers()),
            new ProducerRecord<>(topic, null, timestamp + HEADERS_WINDOW_SIZE_MS, "multi", "v5", headers("z", "4"))
        );
    }

    private static long windowStart(final long timestamp) {
        return timestamp - timestamp % HEADERS_WINDOW_SIZE_MS;
    }

    private static ValueTimestampHeaders<String> valueTimestampHeaders(final ProducerRecord<String, String> record) {
        return ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers());
    }

    private static KeyValue<Windowed<String>, ValueTimestampHeaders<String>> window(final ProducerRecord<String, String> record) {
        final long windowStart = windowStart(record.timestamp());
        return KeyValue.pair(
            new Windowed<>(record.key(), new TimeWindow(windowStart, windowStart + HEADERS_WINDOW_SIZE_MS)),
            valueTimestampHeaders(record));
    }

    private static KeyValue<Windowed<String>, AggregationWithHeaders<String>> session(final ProducerRecord<String, String> record) {
        return KeyValue.pair(
            new Windowed<>(record.key(), new SessionWindow(record.timestamp(), record.timestamp())),
            AggregationWithHeaders.make(record.value(), record.headers()));
    }

    /**
     * @return the value, timestamp, and headers of the latest record of each key, sorted by key
     */
    private static List<KeyValue<String, ValueTimestampHeaders<String>>> latestValuesByKey(final List<ProducerRecord<String, String>> records) {
        final Map<String, ValueTimestampHeaders<String>> latestValues = new TreeMap<>();
        for (final ProducerRecord<String, String> record : records) {
            latestValues.put(record.key(), valueTimestampHeaders(record));
        }
        return latestValues.entrySet().stream()
            .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    }

    private static <T> T store(final KafkaStreams streams,
                               final String storeName,
                               final QueryableStoreType<T> queryableStoreType,
                               final Integer partition,
                               final boolean enableStaleStores) {
        final StoreQueryParameters<T> parameters = fromNameAndType(storeName, queryableStoreType).withPartition(partition);
        return streams.store(enableStaleStores ? parameters.enableStaleStores() : parameters);
    }

    /**
     * Verifies that single-key queries of the stores of {@link #topology} return the value,
     * timestamp, and headers of the records produced for {@code key}.
     */
    private static void verifyForKey(final KafkaStreams streams,
                                     final Integer partition,
                                     final boolean enableStaleStores,
                                     final String key,
                                     final List<ProducerRecord<String, String>> records) {
        final List<ProducerRecord<String, String>> keyRecords = records.stream()
            .filter(record -> record.key().equals(key))
            .collect(Collectors.toList());
        final ProducerRecord<String, String> latestRecord = keyRecords.get(keyRecords.size() - 1);

        final ReadOnlyKeyValueStore<String, ValueTimestampHeaders<String>> kvStore =
            store(streams, HEADERS_KV_STORE, QueryableStoreTypes.timestampedKeyValueStoreWithHeaders(), partition, enableStaleStores);
        assertEquals(valueTimestampHeaders(latestRecord), kvStore.get(key));

        final ReadOnlyWindowStore<String, ValueTimestampHeaders<String>> windowStore =
            store(streams, HEADERS_WINDOW_STORE, QueryableStoreTypes.timestampedWindowStoreWithHeaders(), partition, enableStaleStores);
        assertEquals(
            keyRecords.stream()
                .map(record -> KeyValue.pair(windowStart(record.timestamp()), valueTimestampHeaders(record)))
                .collect(Collectors.toList()),
            toListAndCloseIterator(windowStore.fetch(key, ofEpochMilli(0L), ofEpochMilli(latestRecord.timestamp()))));

        final ReadOnlySessionStore<String, AggregationWithHeaders<String>> sessionStore =
            store(streams, HEADERS_SESSION_STORE, QueryableStoreTypes.sessionStoreWithHeaders(), partition, enableStaleStores);
        assertEquals(
            keyRecords.stream().map(IQv1HeadersStoreIntegrationTest::session).collect(Collectors.toList()),
            toListAndCloseIterator(sessionStore.fetch(key)));
    }

    /**
     * Writes every record, together with its headers, into the stores of {@link #topology}:
     * the window store puts it into the window that contains its timestamp, and the session store into
     * a session that starts and ends at its timestamp.
     */
    private static class Writer implements Processor<String, String, Void, Void> {
        private TimestampedKeyValueStoreWithHeaders<String, String> kvStore;
        private TimestampedWindowStoreWithHeaders<String, String> windowStore;
        private SessionStoreWithHeaders<String, String> sessionStore;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            kvStore = context.getStateStore(HEADERS_KV_STORE);
            windowStore = context.getStateStore(HEADERS_WINDOW_STORE);
            sessionStore = context.getStateStore(HEADERS_SESSION_STORE);
        }

        @Override
        public void process(final Record<String, String> record) {
            final long timestamp = record.timestamp();
            final ValueTimestampHeaders<String> valueTimestampHeaders =
                ValueTimestampHeaders.make(record.value(), timestamp, record.headers());
            kvStore.put(record.key(), valueTimestampHeaders);
            windowStore.put(record.key(), valueTimestampHeaders, windowStart(timestamp));
            sessionStore.put(
                new Windowed<>(record.key(), new SessionWindow(timestamp, timestamp)),
                AggregationWithHeaders.make(record.value(), record.headers()));
        }
    }

    /**
     * Writes every record, together with its headers, into the headers-aware global store of {@link #globalTopology}.
     */
    private static class GlobalWriter implements Processor<String, String, Void, Void> {
        private final String storeName;
        private TimestampedKeyValueStoreWithHeaders<String, String> store;

        GlobalWriter(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
        }
    }

    /**
     * A restore listener that pauses the restoration of a store after its first restored batch, until
     * {@link #resume()} is called.
     */
    private static class PausingRestoreListener implements StateRestoreListener {
        private final String pausedStoreName;
        private final CountDownLatch firstBatchRestored = new CountDownLatch(1);
        private final CountDownLatch resumeRestoration = new CountDownLatch(1);

        PausingRestoreListener(final String pausedStoreName) {
            this.pausedStoreName = pausedStoreName;
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            if (storeName.equals(pausedStoreName) && firstBatchRestored.getCount() > 0) {
                firstBatchRestored.countDown();
                try {
                    resumeRestoration.await(60, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
        }

        void awaitPause() throws InterruptedException {
            assertTrue(firstBatchRestored.await(60, TimeUnit.SECONDS), "the restoration of " + pausedStoreName + " was not paused");
        }

        void resume() {
            resumeRestoration.countDown();
        }
    }
}
