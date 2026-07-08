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
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
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
 * and queries it through IQv2. Covers {@link TimestampedKeyWithHeadersQuery} and
 * {@link TimestampedRangeWithHeadersQuery}; the remaining KIP-1356 headers query types (window/session) are
 * expected to extend this class as they land.
 */
@Tag("integration")
public class IQv2HeadersStoreIntegrationTest {

    private static final String STORE_NAME = "headers-store";

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final Headers HEADERS = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "1.0".getBytes());

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
        startStreamsWithHeadersStore();

        // key 1 has headers, key 2 has empty headers, key 3 is tombstoned (null value)
        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS,
            KeyValue.pair(1, "a0"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 1, new RecordHeaders(),
            KeyValue.pair(2, "b0"));
        produceDataToTopicWithHeaders(inputStream, baseTimestamp + 2, HEADERS,
            KeyValue.pair(3, "c0"), KeyValue.pair(3, null));

        // key 1: key + value + timestamp + headers round-trip
        final ReadOnlyRecord<Integer, String> result1 = query(1);
        assertEquals(Integer.valueOf(1), result1.key());
        assertEquals("a0", result1.value());
        assertEquals(baseTimestamp, result1.timestamp());
        assertEquals(HEADERS, result1.headers());

        // key 2: written with no headers -> empty (never null) headers
        final ReadOnlyRecord<Integer, String> result2 = query(2);
        assertEquals(Integer.valueOf(2), result2.key());
        assertEquals("b0", result2.value());
        assertEquals(baseTimestamp + 1, result2.timestamp());
        assertEquals(new RecordHeaders(), result2.headers());

        // key 3: tombstoned -> null result, never a partially-populated wrapper
        assertNull(query(3));

        // never-written key -> null result
        assertNull(query(999));
    }

    @Test
    public void shouldServeCacheHitWhenCachingEnabledAndRecordNotYetFlushed() throws Exception {
        // Use a very large commit interval so the processed record is never committed/flushed during
        // the test: it lives only in the record cache (the persistent RocksDB layer stays empty). A
        // successful query therefore proves the result was served from the cache via
        // CachingKeyValueStoreWithHeaders -> the metered store's cache-hit path, end-to-end.
        commitIntervalMs = Duration.ofMinutes(10).toMillis();
        startStreamsWithHeadersStore(true);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        // Read-your-writes: the not-yet-flushed record is visible (served from the cache), with headers.
        final ReadOnlyRecord<Integer, String> result = query(1);
        // skipCache bypasses the cache and reads the persistent store directly. A null result positively
        // proves nothing has been flushed, so the read above was genuinely cache-served (not an accidental
        // store read) -- and it covers skipCache end-to-end.
        assertNull(querySkipCache(1));
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
        startStreamsWithHeadersStore();

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
    }

    @Test
    public void shouldFailWithUnknownQueryTypeForRangeQueryAgainstNonHeadersStore() throws Exception {
        assertUnknownQueryTypeAgainstNonHeadersStore(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds());
    }

    @Test
    public void shouldThrowForTimestampedRangeWithHeadersQueryOnPlainSupplier() throws Exception {
        // The query succeeds, but iterating throws because timestamp = -1 cannot be a ReadOnlyRecord.
        startStreamsWithPlainSupplierStore();

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

    private void startStreamsWithHeadersStore() throws Exception {
        // Caching disabled: every IQv2 query is forced down to the persistent
        // RocksDBTimestampedStoreWithHeaders layer, exercising its KeyQuery handling
        // (rather than being short-circuited by a cache hit).
        startStreamsWithHeadersStore(false);
    }

    private void startStreamsWithHeadersStore(final boolean cachingEnabled) throws Exception {
        final StoreBuilder<TimestampedKeyValueStoreWithHeaders<Integer, String>> storeBuilder =
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                Serdes.Integer(),
                Serdes.String());
        startStreams(
            cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled(),
            HeadersStoreWriterProcessor::new);
    }

    private void startStreamsWithNonHeadersStore() throws Exception {
        // A plain (non-WithHeaders) timestamped store: the headers-aware query types are unsupported here.
        startStreams(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.String()),
            PlainStoreWriterProcessor::new);
    }

    private void startStreamsWithPlainSupplierStore() throws Exception {
        // A WithHeaders builder over a plain (non-timestamped) supplier: entries come back with
        // timestamp = -1, which cannot be represented as a ReadOnlyRecord.
        startStreams(
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.Integer(),
                Serdes.String()).withCachingDisabled(),
            HeadersStoreWriterProcessor::new);
    }

    private <R> void assertUnknownQueryTypeAgainstNonHeadersStore(final Query<R> query) throws Exception {
        startStreamsWithNonHeadersStore();
        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        final StateQueryRequest<R> request =
            inStore(STORE_NAME).withQuery(query).withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<R> result = IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        assertTrue(result.getOnlyPartitionResult().isFailure());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getOnlyPartitionResult().getFailureReason());
    }

    private ReadOnlyRecord<Integer, String> query(final int key) {
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

    private ReadOnlyRecord<Integer, String> querySkipCache(final int key) {
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

    private static class HeadersStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
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

    private static class PlainStoreWriterProcessor implements Processor<Integer, String, Integer, String> {
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
}
