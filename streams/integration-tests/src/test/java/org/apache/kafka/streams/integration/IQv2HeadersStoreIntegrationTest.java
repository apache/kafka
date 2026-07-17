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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedRangeWithHeadersQuery;
import org.apache.kafka.streams.query.TimestampedWindowKeyWithHeadersQuery;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQv2 integration tests for KIP-1271/KIP-1356 headers-aware state stores.
 *
 * <p>It builds a KIP-1271 {@code WithHeaders} store, writes records (with headers) into it through a processor,
 * and queries it through IQv2. Covers {@link TimestampedKeyWithHeadersQuery},
 * {@link TimestampedRangeWithHeadersQuery}, and {@link TimestampedWindowKeyWithHeadersQuery}; the remaining
 * KIP-1356 headers query type (window range / session) is expected to extend this class as it lands.
 */
@Tag("integration")
public class IQv2HeadersStoreIntegrationTest {

    private static final String STORE_NAME = "headers-store";

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final Headers HEADERS = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "1.0".getBytes());

    // Window-store test parameters (used by the window headers query tests).
    private static final long WINDOW_SIZE_MS = 10L;
    // Stored event-time is offset a few ms into the window so tests can tell ReadOnlyRecord.timestamp()
    // (the event-time) apart from the window's start and end. Must satisfy 0 < offset < WINDOW_SIZE_MS.
    private static final long EVENT_TIME_OFFSET_MS = 3L;
    private static final Duration WINDOW_SIZE = Duration.ofMillis(WINDOW_SIZE_MS);
    private static final Duration RETENTION = Duration.ofMinutes(1);

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;
    private long commitIntervalMs = 1000L;
    private Position inputPosition;
    private KafkaStreams kafkaStreams;
    private TestInfo testInfo;

    @BeforeAll
    public static void before() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void after() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void beforeTest(final TestInfo testInfo) throws InterruptedException {
        this.testInfo = testInfo;
        final String uniqueTestName = safeUniqueTestName(testInfo);
        inputStream = "input-stream-" + uniqueTestName;
        outputStream = "output-stream-" + uniqueTestName;
        CLUSTER.createTopic(inputStream);
        CLUSTER.createTopic(outputStream);
        baseTimestamp = CLUSTER.time.milliseconds();
        inputPosition = Position.emptyPosition();
    }

    @AfterEach
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldHandleTimestampedKeyWithHeadersQuery() throws Exception {
        startStreamsWithKeyValueHeadersStore();

        // key 1 has headers, key 2 has empty headers, key 3 is tombstoned (null value)
        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS,
            KeyValue.pair(1, "a0"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 1, new RecordHeaders(),
            KeyValue.pair(2, "b0"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2, HEADERS,
            KeyValue.pair(3, "c0"), KeyValue.pair(3, null));

        // key 1: key + value + timestamp + headers round-trip
        final ReadOnlyRecord<Integer, String> result1 = keyQuery(1);
        assertEquals(Integer.valueOf(1), result1.key());
        assertEquals("a0", result1.value());
        assertEquals(baseTimestamp, result1.timestamp());
        assertEquals(HEADERS, result1.headers());

        // key 2: written with no headers -> empty (never null) headers
        final ReadOnlyRecord<Integer, String> result2 = keyQuery(2);
        assertEquals(Integer.valueOf(2), result2.key());
        assertEquals("b0", result2.value());
        assertEquals(baseTimestamp + 1, result2.timestamp());
        assertEquals(new RecordHeaders(), result2.headers());

        // key 3: tombstoned -> null result, never a partially-populated wrapper
        assertNull(keyQuery(3));

        // never-written key -> null result
        assertNull(keyQuery(999));
    }

    @Test
    public void shouldServeCacheHitWhenCachingEnabledAndRecordNotYetFlushed() throws Exception {
        // Use a very large commit interval so the processed record is never committed/flushed during
        // the test: it lives only in the record cache (the persistent RocksDB layer stays empty). A
        // successful query therefore proves the result was served from the cache via
        // CachingKeyValueStoreWithHeaders -> the metered store's cache-hit path, end-to-end.
        commitIntervalMs = Duration.ofMinutes(10).toMillis();
        startStreamsWithKeyValueHeadersStore(true);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        // Read-your-writes: the not-yet-flushed record is visible (served from the cache), with headers.
        final ReadOnlyRecord<Integer, String> result = keyQuery(1);
        // skipCache bypasses the cache and reads the persistent store directly. A null result positively
        // proves nothing has been flushed, so the read above was genuinely cache-served (not an accidental
        // store read) -- and it covers skipCache end-to-end.
        assertNull(keyQuerySkipCache(1));
        assertEquals(Integer.valueOf(1), result.key());
        assertEquals("a0", result.value());
        assertEquals(baseTimestamp, result.timestamp());
        assertEquals(HEADERS, result.headers());
    }

    @Test
    public void shouldFailWithUnknownQueryTypeForKeyQueryAgainstNonHeadersStore() throws Exception {
        assertUnknownQueryTypeAgainstNonHeadersStore(TimestampedKeyWithHeadersQuery.<Integer, String>withKey(1));
    }

    @Test
    public void shouldHandleTimestampedRangeWithHeadersQuery() throws Exception {
        // Caching disabled: a range query reads the underlying store directly (it never consults the
        // cache), so the writes must be store-served.
        startStreamsWithKeyValueHeadersStore();

        // keys 1,2 (headers), key 3 (empty headers), key 4 written then tombstone
        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS,
            KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 1, new RecordHeaders(),
            KeyValue.pair(3, "three"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2, HEADERS,
            KeyValue.pair(4, "four"), KeyValue.pair(4, null));

        // Full scan, ascending: keys 1, 2, 3 (key 4 tombstone and omitted).
        final List<ReadOnlyRecord<Integer, String>> ascending =
            rangeQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds().withAscendingKeys());
        assertEquals(List.of(1, 2, 3), keys(ascending));
        assertEquals(List.of("one", "two", "three"), values(ascending));
        // key/value/timestamp/headers carried on each element
        assertEquals(HEADERS, ascending.get(0).headers());          // key 1
        assertEquals(baseTimestamp, ascending.get(0).timestamp());
        assertEquals(HEADERS, ascending.get(1).headers());          // key 2
        assertEquals(baseTimestamp, ascending.get(1).timestamp());
        // key 3 written without headers -> empty (never null) headers
        assertEquals(new RecordHeaders(), ascending.get(2).headers());
        assertEquals(baseTimestamp + 1, ascending.get(2).timestamp());
        // returned headers are a read-only snapshot: no mutation (add or remove) is allowed
        assertThrows(IllegalStateException.class, () -> ascending.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> ascending.get(0).headers().remove("source"));

        // Full scan, descending: keys reversed, payload still carried.
        final List<ReadOnlyRecord<Integer, String>> descending =
            rangeQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds().withDescendingKeys());
        assertEquals(List.of(3, 2, 1), keys(descending));
        assertEquals(List.of("three", "two", "one"), values(descending));
        // key/value/timestamp/headers carried on each element
        // key 3 written without headers -> empty (never null) headers
        assertEquals(new RecordHeaders(), descending.get(0).headers());  // key 3
        assertEquals(baseTimestamp + 1, descending.get(0).timestamp());
        assertEquals(HEADERS, descending.get(1).headers());             // key 2
        assertEquals(baseTimestamp, descending.get(1).timestamp());
        assertEquals(HEADERS, descending.get(2).headers());             // key 1
        assertEquals(baseTimestamp, descending.get(2).timestamp());
        // returned headers are a read-only snapshot: no mutation (add or remove) is allowed
        assertThrows(IllegalStateException.class, () -> descending.get(2).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> descending.get(2).headers().remove("source"));

        // Bounded ranges (inclusive on both ends); same per-element checks as the full scans.
        // withRange(2, 3) -> keys 2, 3.
        final List<ReadOnlyRecord<Integer, String>> range = rangeQuery(TimestampedRangeWithHeadersQuery.withRange(2, 3));
        assertEquals(List.of(2, 3), keys(range));
        assertEquals(List.of("two", "three"), values(range));
        assertEquals(HEADERS, range.get(0).headers());                  // key 2
        assertEquals(baseTimestamp, range.get(0).timestamp());
        // key 3 written without headers -> empty (never null) headers
        assertEquals(new RecordHeaders(), range.get(1).headers());      // key 3
        assertEquals(baseTimestamp + 1, range.get(1).timestamp());
        // returned headers are a read-only snapshot: no mutation (add or remove) is allowed
        assertThrows(IllegalStateException.class, () -> range.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> range.get(0).headers().remove("source"));

        // withLowerBound(2) -> keys 2, 3.
        final List<ReadOnlyRecord<Integer, String>> lowerBounded =
            rangeQuery(TimestampedRangeWithHeadersQuery.withLowerBound(2));
        assertEquals(List.of(2, 3), keys(lowerBounded));
        assertEquals(List.of("two", "three"), values(lowerBounded));
        assertEquals(HEADERS, lowerBounded.get(0).headers());           // key 2
        assertEquals(baseTimestamp, lowerBounded.get(0).timestamp());
        assertEquals(new RecordHeaders(), lowerBounded.get(1).headers()); // key 3
        assertEquals(baseTimestamp + 1, lowerBounded.get(1).timestamp());
        assertThrows(IllegalStateException.class, () -> lowerBounded.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> lowerBounded.get(0).headers().remove("source"));

        // withUpperBound(2) -> keys 1, 2.
        final List<ReadOnlyRecord<Integer, String>> upperBounded =
            rangeQuery(TimestampedRangeWithHeadersQuery.withUpperBound(2));
        assertEquals(List.of(1, 2), keys(upperBounded));
        assertEquals(List.of("one", "two"), values(upperBounded));
        assertEquals(HEADERS, upperBounded.get(0).headers());           // key 1
        assertEquals(baseTimestamp, upperBounded.get(0).timestamp());
        assertEquals(HEADERS, upperBounded.get(1).headers());           // key 2
        assertEquals(baseTimestamp, upperBounded.get(1).timestamp());
        assertThrows(IllegalStateException.class, () -> upperBounded.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> upperBounded.get(0).headers().remove("source"));

        // withRange(2, 2), equal bounds -> key 2 only (bounds are inclusive on both ends).
        final List<ReadOnlyRecord<Integer, String>> equalBounds = rangeQuery(TimestampedRangeWithHeadersQuery.withRange(2, 2));
        assertEquals(List.of(2), keys(equalBounds));
        assertEquals(List.of("two"), values(equalBounds));
        assertEquals(HEADERS, equalBounds.get(0).headers());
        assertEquals(baseTimestamp, equalBounds.get(0).timestamp());
        assertThrows(IllegalStateException.class, () -> equalBounds.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> equalBounds.get(0).headers().remove("source"));

        // withRange(2, 3), descending -> keys 3, 2.
        final List<ReadOnlyRecord<Integer, String>> rangeDescending =
            rangeQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withRange(2, 3).withDescendingKeys());
        assertEquals(List.of(3, 2), keys(rangeDescending));
        assertEquals(List.of("three", "two"), values(rangeDescending));
        // key 3 written without headers -> empty (never null) headers
        assertEquals(new RecordHeaders(), rangeDescending.get(0).headers()); // key 3
        assertEquals(baseTimestamp + 1, rangeDescending.get(0).timestamp());
        assertEquals(HEADERS, rangeDescending.get(1).headers());            // key 2
        assertEquals(baseTimestamp, rangeDescending.get(1).timestamp());
        // returned headers are a read-only snapshot: no mutation (add or remove) is allowed
        assertThrows(IllegalStateException.class, () -> rangeDescending.get(1).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> rangeDescending.get(1).headers().remove("source"));

        // A bound matching no keys -> empty result (the no-match path).
        final List<ReadOnlyRecord<Integer, String>> noMatch =
            rangeQuery(TimestampedRangeWithHeadersQuery.withRange(4, 200));
        assertTrue(noMatch.isEmpty());

        // withRange(3, 2), lower > upper -> empty result (an inverted range matches nothing, rather than failing).
        final List<ReadOnlyRecord<Integer, String>> invertedBounds = rangeQuery(TimestampedRangeWithHeadersQuery.withRange(3, 2));
        assertTrue(invertedBounds.isEmpty());
    }

    @Test
    public void shouldNotSeeUnflushedWriteInRangeQueryWhenCachingEnabled() throws Exception {
        // Unlike the point query (CachingKeyValueStoreWithHeaders serves KeyQuery/TimestampedKeyWithHeadersQuery
        // from the cache), CachingKeyValueStoreWithHeaders.query() forwards RangeQuery/TimestampedRangeWithHeadersQuery
        // straight to the underlying store, bypassing the cache. That underlying store's Position is only
        // advanced by writes that actually reach it, so a range query bound on the input position never catches
        // up while the write lives only in the cache -- it fails NOT_UP_TO_BOUND, rather than returning an empty
        // (but successful) result. Use a very large commit interval so the record is never flushed during the test.
        commitIntervalMs = Duration.ofMinutes(10).toMillis();
        startStreamsWithKeyValueHeadersStore(true);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        final StateQueryRequest<ReadOnlyRecordIterator<Integer, String>> request =
                inStore(STORE_NAME)
                        .withQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds())
                        .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Integer, String>> result = kafkaStreams.query(request);

        final QueryResult<ReadOnlyRecordIterator<Integer, String>> onlyResult = result.getOnlyPartitionResult();
        assertTrue(onlyResult.isFailure(), "A range query bound on the input position must not catch up while the write is cache-only");
        assertEquals(FailureReason.NOT_UP_TO_BOUND, onlyResult.getFailureReason());
    }

    @Test
    public void shouldFailWithUnknownQueryTypeForRangeQueryAgainstNonHeadersStore() throws Exception {
        assertUnknownQueryTypeAgainstNonHeadersStore(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds());
    }

    @Test
    public void shouldThrowForTimestampedRangeWithHeadersQueryOnPlainSupplier() throws Exception {
        // The query succeeds, but iterating throws because timestamp = -1 cannot be a ReadOnlyRecord.
        startStreamsWithKeyValuePlainSupplierStore();

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS,
            KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));

        final StateQueryRequest<ReadOnlyRecordIterator<Integer, String>> request =
            inStore(STORE_NAME)
                .withQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds())
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final QueryResult<ReadOnlyRecordIterator<Integer, String>> onlyResult = result.getOnlyPartitionResult();
        assertTrue(onlyResult.isSuccess());
        try (ReadOnlyRecordIterator<Integer, String> iterator = onlyResult.getResult()) {
            assertThrows(StreamsException.class, iterator::next);
        }
    }

    @Test
    public void shouldReturnEmptyHeadersForTimestampedRangeWithHeadersQueryOnAdapterStore() throws Exception {
        // A WithHeaders builder over a plain *timestamped* supplier (persistentTimestampedKeyValueStore)
        // keeps timestamps but drops headers -- distinct from the plain-supplier build above, which can't
        // even represent the timestamp. A range query reads the underlying store directly, so headers
        // come back empty (never null), even though they were written.
        startStreamsWithKeyValueAdapterStore();

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "one"));

        final List<ReadOnlyRecord<Integer, String>> range =
            rangeQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds());
        assertEquals(List.of(1), keys(range));
        assertEquals(List.of("one"), values(range));
        assertEquals(baseTimestamp, range.get(0).timestamp());
        assertEquals(new RecordHeaders(), range.get(0).headers());
        // returned headers are a read-only snapshot: no mutation (add or remove) is allowed, even when empty
        assertThrows(IllegalStateException.class, () -> range.get(0).headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> range.get(0).headers().remove("source"));
    }

    @Test
    public void shouldHandleTimestampedWindowKeyWithHeadersQuery() throws Exception {
        startStreamsWithWindowHeadersStore();

        final long window0 = baseTimestamp;
        final long window1 = baseTimestamp + 100;
        final long window2 = baseTimestamp + 200;
        final long window3 = baseTimestamp + 300;
        final long window4 = baseTimestamp + 400;

        // Queried key 1 across four window starts: headers, empty headers, tombstoned, headers.
        produceDataToTopicWithHeaders(inputStream, window0, HEADERS, KeyValue.pair(1, "v0"));
        produceDataToTopicWithHeaders(inputStream, window1, new RecordHeaders(), KeyValue.pair(1, "v1"));
        produceDataToTopicWithHeaders(inputStream, window2, HEADERS, KeyValue.pair(1, "v2"), KeyValue.pair(1, null));
        produceDataToTopicWithHeaders(inputStream, window3, HEADERS, KeyValue.pair(1, "v3"));

        // Noise key 2 must never leak into a query for key 1: a tombstone at window0 (which must not remove
        // key 1's window0), a live entry at the shared window1, and a live entry at window4 (a window key 1
        // never uses).
        produceDataToTopicWithHeaders(inputStream, window0, HEADERS, KeyValue.pair(2, "o0"), KeyValue.pair(2, null));
        produceDataToTopicWithHeaders(inputStream, window1, HEADERS, KeyValue.pair(2, "o1"));
        produceDataToTopicWithHeaders(inputStream, window4, HEADERS, KeyValue.pair(2, "o4"));

        // Full range over key 1: window0, window1, window3 in window-start order. window2 is tombstoned;
        // key 2's entries are excluded; and key 2's window0 tombstone did not drop key 1's window0.
        final List<ReadOnlyRecord<Windowed<Integer>, String>> records =
                windowKeyQuery(1, Instant.ofEpochMilli(window0), Instant.ofEpochMilli(window4));
        assertEquals(
                List.of(window0, window1, window3),
                records.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "expected window0, window1, window3 in order (window2 tombstoned, key 2 excluded)");
        assertWindowedRecord(records.get(0), 1, window0, "v0", HEADERS);
        assertWindowedRecord(records.get(1), 1, window1, "v1", new RecordHeaders());
        assertWindowedRecord(records.get(2), 1, window3, "v3", HEADERS);

        // Sub-range [window1, window2]: only window1 (window0 is below the lower bound, window2 is
        // tombstoned, window3 is above the upper bound).
        final List<ReadOnlyRecord<Windowed<Integer>, String>> subRange =
                windowKeyQuery(1, Instant.ofEpochMilli(window1), Instant.ofEpochMilli(window2));
        assertEquals(1, subRange.size());
        assertWindowedRecord(subRange.get(0), 1, window1, "v1", new RecordHeaders());

        // A never-written key -> empty result.
        assertTrue(windowKeyQuery(999, Instant.ofEpochMilli(window0), Instant.ofEpochMilli(window4)).isEmpty(),
                "expected no records for a never-written key");
        // key 1, a window-start range entirely after its last window -> empty result.
        assertTrue(windowKeyQuery(1, Instant.ofEpochMilli(window3 + WINDOW_SIZE_MS), Instant.ofEpochMilli(window4)).isEmpty(),
                "expected no records for a window-start range that excludes all of key 1's windows");

        // Querying key 2 returns only its live entries (window1, window4; window0 tombstoned) -- isolation is
        // bidirectional and key 2's own tombstone is key-scoped.
        final List<ReadOnlyRecord<Windowed<Integer>, String>> key2Records =
                windowKeyQuery(2, Instant.ofEpochMilli(window0), Instant.ofEpochMilli(window4));
        assertEquals(
                List.of(window1, window4),
                key2Records.stream().map(r -> r.key().window().start()).collect(Collectors.toList()),
                "expected key 2's window1 and window4 (window0 tombstoned)");
        assertWindowedRecord(key2Records.get(0), 2, window1, "o1", HEADERS);
        assertWindowedRecord(key2Records.get(1), 2, window4, "o4", HEADERS);
    }

    @Test
    public void shouldNotSeeUnflushedWriteInWindowKeyQueryWhenCachingEnabled() throws Exception {
        // A window key query bypasses the record cache: CachingWindowStore does not intercept IQv2 queries
        // (unlike CachingKeyValueStore), so WrappedStateStore.query() forwards straight to the persistent
        // store. That store's Position is only advanced by writes that actually reach it, so a query bound on
        // the input position never catches up while the write lives only in the cache -- it fails
        // NOT_UP_TO_BOUND, rather than returning a result. Use a very large commit interval so the record is
        // never flushed during the test.
        commitIntervalMs = Duration.ofMinutes(10).toMillis();
        startStreamsWithWindowHeadersStore(true);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "v0"));

        final StateQueryRequest<ReadOnlyRecordIterator<Windowed<Integer>, String>> request =
                inStore(STORE_NAME)
                        .withQuery(TimestampedWindowKeyWithHeadersQuery.<Integer, String>withKeyAndWindowStartRange(
                                1, Instant.ofEpochMilli(baseTimestamp), Instant.ofEpochMilli(baseTimestamp)))
                        .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Windowed<Integer>, String>> result = kafkaStreams.query(request);

        final QueryResult<ReadOnlyRecordIterator<Windowed<Integer>, String>> onlyResult = result.getOnlyPartitionResult();
        assertTrue(onlyResult.isFailure(), "A window key query bound on the input position must not catch up while the write is cache-only");
        assertEquals(FailureReason.NOT_UP_TO_BOUND, onlyResult.getFailureReason());
    }

    @Test
    public void shouldFailWithUnknownQueryTypeForWindowKeyQueryAgainstNonHeadersStore() throws Exception {
        startStreamsWithWindowNonHeadersStore();
        assertUnknownQueryType(TimestampedWindowKeyWithHeadersQuery.<Integer, String>withKeyAndWindowStartRange(
                1, Instant.ofEpochMilli(baseTimestamp), Instant.ofEpochMilli(baseTimestamp + 1)));
    }

    @Test
    public void shouldThrowForTimestampedWindowKeyWithHeadersQueryOnPlainSupplier() throws Exception {
        // A WithHeaders window builder over a plain (non-timestamped) window supplier: entries come back
        // with timestamp = -1. The query succeeds, but iterating throws because -1 cannot be a ReadOnlyRecord.
        startStreamsWithWindowPlainSupplierStore();

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "one"));

        final StateQueryRequest<ReadOnlyRecordIterator<Windowed<Integer>, String>> request =
            inStore(STORE_NAME)
                .withQuery(TimestampedWindowKeyWithHeadersQuery.<Integer, String>withKeyAndWindowStartRange(
                    1, Instant.ofEpochMilli(baseTimestamp), Instant.ofEpochMilli(baseTimestamp + 1)))
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Windowed<Integer>, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final QueryResult<ReadOnlyRecordIterator<Windowed<Integer>, String>> onlyResult = result.getOnlyPartitionResult();
        assertTrue(onlyResult.isSuccess());
        try (ReadOnlyRecordIterator<Windowed<Integer>, String> iterator = onlyResult.getResult()) {
            final StreamsException e = assertThrows(StreamsException.class, iterator::next);
            assertTrue(e.getMessage().contains("as a ReadOnlyRecord") && e.getMessage().contains("is negative"),
                "unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void shouldReturnEmptyHeadersForTimestampedWindowKeyWithHeadersQueryOnAdapterStore() throws Exception {
        // A WithHeaders window builder over a plain *timestamped* window supplier keeps timestamps
        // (unlike the plain-supplier build above) but drops headers via
        // TimestampedToHeadersWindowStoreAdapter. A window key query reads the underlying store
        // directly, so headers come back empty (never null), even though they were written.
        startStreamsWithWindowAdapterStore();

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "one"));

        final List<ReadOnlyRecord<Windowed<Integer>, String>> records =
                windowKeyQuery(1, Instant.ofEpochMilli(baseTimestamp), Instant.ofEpochMilli(baseTimestamp));
        assertEquals(1, records.size());
        assertWindowedRecord(records.get(0), 1, baseTimestamp, "one", new RecordHeaders());
    }

    private void startStreams(final StoreBuilder<?> storeBuilder,
                              final ProcessorSupplier<Integer, String, Integer, String> processorSupplier) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder
                .addStateStore(storeBuilder)
                .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
                .process(processorSupplier, STORE_NAME)
                .to(outputStream, Produced.with(Serdes.Integer(), Serdes.String()));

        kafkaStreams = new KafkaStreams(builder.build(), props());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
    }

    private void startStreamsWithKeyValueHeadersStore() throws Exception {
        // Caching disabled: every IQv2 query is forced down to the persistent
        // RocksDBTimestampedStoreWithHeaders layer, exercising its KeyQuery handling
        // (rather than being short-circuited by a cache hit).
        startStreamsWithKeyValueHeadersStore(false);
    }

    private void startStreamsWithKeyValueHeadersStore(final boolean cachingEnabled) throws Exception {
        final StoreBuilder<TimestampedKeyValueStoreWithHeaders<Integer, String>> storeBuilder =
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                Serdes.Integer(),
                Serdes.String());
        startStreams(
            cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled(),
            KeyValueHeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithKeyValueNonHeadersStore() throws Exception {
        // A plain (non-WithHeaders) timestamped store: the headers-aware query types are unsupported here.
        startStreams(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.String()),
            KeyValuePlainStoreWriterProcessor::new);
    }

    private void startStreamsWithKeyValuePlainSupplierStore() throws Exception {
        // A WithHeaders builder over a plain (non-timestamped) supplier: entries come back with
        // timestamp = -1, which cannot be represented as a ReadOnlyRecord.
        startStreams(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.String()).withCachingDisabled(),
            KeyValueHeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithKeyValueAdapterStore() throws Exception {
        // A WithHeaders builder over a plain *timestamped* supplier: keeps timestamps (unlike the
        // plain-supplier build above) but drops headers via TimestampedToHeadersStoreAdapter.
        startStreams(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.String()).withCachingDisabled(),
            KeyValueHeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithWindowHeadersStore() throws Exception {
        // Caching disabled: every IQv2 query is forced down to the persistent
        // RocksDBTimestampedWindowStoreWithHeaders layer.
        startStreamsWithWindowHeadersStore(false);
    }

    private void startStreamsWithWindowHeadersStore(final boolean cachingEnabled) throws Exception {
        final StoreBuilder<TimestampedWindowStoreWithHeaders<Integer, String>> storeBuilder =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.String());
        startStreams(
            cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled(),
            WindowHeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithWindowNonHeadersStore() throws Exception {
        // A plain (non-WithHeaders) timestamped window store: the headers-aware query types are unsupported here.
        startStreams(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.String()),
            WindowPlainStoreWriterProcessor::new);
    }

    private void startStreamsWithWindowPlainSupplierStore() throws Exception {
        // A WithHeaders window builder over a plain (non-timestamped) window supplier: entries come back
        // with timestamp = -1, which cannot be represented as a ReadOnlyRecord.
        startStreams(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.String()).withCachingDisabled(),
            WindowHeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithWindowAdapterStore() throws Exception {
        // A WithHeaders window builder over a plain *timestamped* window supplier: the adapter keeps
        // timestamps but cannot persist headers, so window key results carry value + timestamp with empty
        // headers.
        startStreams(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                Serdes.Integer(),
                Serdes.String()).withCachingDisabled(),
            WindowHeadersStoreWriterProcessor::new);
    }

    private <R> void assertUnknownQueryTypeAgainstNonHeadersStore(final Query<R> query) throws Exception {
        startStreamsWithKeyValueNonHeadersStore();
        assertUnknownQueryType(query);
    }

    private <R> void assertUnknownQueryType(final Query<R> query) {
        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        final StateQueryRequest<R> request =
            inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<R> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        assertTrue(result.getOnlyPartitionResult().isFailure());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getOnlyPartitionResult().getFailureReason());
    }

    private ReadOnlyRecord<Integer, String> keyQuery(final int key) {
        final StateQueryRequest<ReadOnlyRecord<Integer, String>> request =
            inStore(STORE_NAME)
                .withQuery(TimestampedKeyWithHeadersQuery.<Integer, String>withKey(key))
                .withPositionBound(PositionBound.at(inputPosition));
        // Retry until the store has caught up to the produced input position; freshness comes from the
        // IQv2 position mechanism rather than from output-topic consumption.
        final StateQueryResult<ReadOnlyRecord<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        // getOnlyPartitionResult() returns null when the single partition result is a successful
        // null (tombstoned / absent key), which we surface to the caller as a null lookup.
        final QueryResult<ReadOnlyRecord<Integer, String>> onlyResult = result.getOnlyPartitionResult();
        return onlyResult == null ? null : onlyResult.getResult();
    }

    private ReadOnlyRecord<Integer, String> keyQuerySkipCache(final int key) {
        // skipCache forwards the query past the record cache to the persistent store. Use the default
        // (unbounded) position bound on purpose: the persistent layer may legitimately be empty (nothing
        // flushed), so bounding on the input position would never be satisfied.
        final StateQueryRequest<ReadOnlyRecord<Integer, String>> request =
            inStore(STORE_NAME).withQuery(
                TimestampedKeyWithHeadersQuery.<Integer, String>withKey(key).skipCache());
        final StateQueryResult<ReadOnlyRecord<Integer, String>> result = kafkaStreams.query(request);
        final QueryResult<ReadOnlyRecord<Integer, String>> onlyResult = result.getOnlyPartitionResult();
        return onlyResult == null ? null : onlyResult.getResult();
    }

    private List<ReadOnlyRecord<Integer, String>> rangeQuery(final TimestampedRangeWithHeadersQuery<Integer, String> query) {
        final StateQueryRequest<ReadOnlyRecordIterator<Integer, String>> request =
            inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        final List<ReadOnlyRecord<Integer, String>> records = new ArrayList<>();
        try (ReadOnlyRecordIterator<Integer, String> iterator = result.getOnlyPartitionResult().getResult()) {
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
        }
        return records;
    }

    private List<ReadOnlyRecord<Windowed<Integer>, String>> windowKeyQuery(final int key,
                                                                           final Instant timeFrom,
                                                                           final Instant timeTo) {
        final StateQueryRequest<ReadOnlyRecordIterator<Windowed<Integer>, String>> request =
            inStore(STORE_NAME)
                .withQuery(TimestampedWindowKeyWithHeadersQuery.<Integer, String>withKeyAndWindowStartRange(
                    key, timeFrom, timeTo))
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Windowed<Integer>, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);
        final List<ReadOnlyRecord<Windowed<Integer>, String>> records = new ArrayList<>();
        try (ReadOnlyRecordIterator<Windowed<Integer>, String> iterator = result.getOnlyPartitionResult().getResult()) {
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
        }
        return records;
    }

    private void assertWindowedRecord(final ReadOnlyRecord<Windowed<Integer>, String> record,
                                      final int key,
                                      final long windowStart,
                                      final String value,
                                      final Headers expectedHeaders) {
        assertEquals(Integer.valueOf(key), record.key().key());
        assertEquals(windowStart, record.key().window().start());
        assertEquals(windowStart + WINDOW_SIZE_MS, record.key().window().end());
        // timestamp() is the stored event-time, distinct from the window's start and end.
        assertEquals(windowStart + EVENT_TIME_OFFSET_MS, record.timestamp());
        assertEquals(value, record.value());
        assertEquals(expectedHeaders, record.headers());
        // The IQ result is a read-only snapshot: neither add nor remove is allowed.
        assertThrows(IllegalStateException.class, () -> record.headers().add("x", new byte[0]));
        assertThrows(IllegalStateException.class, () -> record.headers().remove("source"));
    }

    private static List<Integer> keys(final List<ReadOnlyRecord<Integer, String>> records) {
        return records.stream().map(ReadOnlyRecord::key).collect(Collectors.toList());
    }

    private static List<String> values(final List<ReadOnlyRecord<Integer, String>> records) {
        return records.stream().map(ReadOnlyRecord::value).collect(Collectors.toList());
    }

    private Properties props() {
        final String safeTestName = safeUniqueTestName(testInfo);
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @SuppressWarnings("varargs")
    @SafeVarargs
    private final void produceDataToTopicWithHeaders(final String topic,
                                                     final long timestamp,
                                                     final Headers headers,
                                                     final KeyValue<Integer, String>... keyValues) {
        final Properties producerConfig =
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, StringSerializer.class);
        final List<Future<RecordMetadata>> futures = new LinkedList<>();
        try (final Producer<Integer, String> producer = new KafkaProducer<>(producerConfig)) {
            for (final KeyValue<Integer, String> keyValue : keyValues) {
                futures.add(producer.send(
                    new ProducerRecord<>(topic, null, timestamp, keyValue.key, keyValue.value, headers)));
            }
            producer.flush();
            for (final Future<RecordMetadata> future : futures) {
                try {
                    final RecordMetadata metadata = future.get(60, TimeUnit.SECONDS);
                    // Track the produced input Position so queries can bound on it (IQv2 freshness).
                    inputPosition = inputPosition.withComponent(
                        metadata.topic(), metadata.partition(), metadata.offset());
                } catch (final Exception e) {
                    throw new RuntimeException("Failed to produce test record to " + topic, e);
                }
            }
        }
    }

    private static class KeyValueHeadersStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
        private ProcessorContext<Integer, String> context;
        private TimestampedKeyValueStoreWithHeaders<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, String> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            if (record.value() == null) {
                store.delete(record.key());
            } else {
                store.put(
                    record.key(),
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
            }
            context.forward(record);
        }
    }

    private static class KeyValuePlainStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
        private ProcessorContext<Integer, String> context;
        private TimestampedKeyValueStore<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, String> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            store.put(
                record.key(),
                ValueAndTimestamp.make(record.value(), record.timestamp()));
            context.forward(record);
        }
    }

    private static class WindowHeadersStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
        private ProcessorContext<Integer, String> context;
        private TimestampedWindowStoreWithHeaders<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, String> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            // The record timestamp is the window start; a null value tombstones that window. The value's
            // event-time is stored a few ms into the window (EVENT_TIME_OFFSET_MS) so tests can tell
            // ReadOnlyRecord.timestamp() apart from the window start/end.
            if (record.value() == null) {
                store.put(record.key(), null, record.timestamp());
            } else {
                store.put(
                    record.key(),
                    ValueTimestampHeaders.make(record.value(), record.timestamp() + EVENT_TIME_OFFSET_MS, record.headers()),
                    record.timestamp());
            }
            context.forward(record);
        }
    }

    private static class WindowPlainStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
        private ProcessorContext<Integer, String> context;
        private TimestampedWindowStore<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, String> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            store.put(
                record.key(),
                ValueAndTimestamp.make(record.value(), record.timestamp()),
                record.timestamp());
            context.forward(record);
        }
    }
}
