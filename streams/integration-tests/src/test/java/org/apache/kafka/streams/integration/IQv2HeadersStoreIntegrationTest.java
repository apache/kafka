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
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQv2 integration tests for KIP-1271/KIP-1356 headers-aware state stores.
 *
 * <p>It builds a KIP-1271 {@code WithHeaders} store, writes records (with headers) into it through a processor,
 * and queries it through IQv2. Currently covers {@link TimestampedKeyWithHeadersQuery}; the remaining KIP-1356 headers
 * query types (range/window/session) are expected to extend this class as they land.
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
        startStreams();

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
        startStreams(true);

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
    public void shouldFailWithUnknownQueryTypeAgainstNonHeadersStore() throws Exception {
        // store built WITHOUT a WithHeaders supplier -> the query type is unsupported
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new PlainStoreWriterProcessor(), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.String()));

        kafkaStreams = new KafkaStreams(builder.build(), props());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);

        produceDataToTopicWithHeaders(inputStream, baseTimestamp, HEADERS, KeyValue.pair(1, "a0"));

        final StateQueryRequest<ReadOnlyRecord<Integer, String>> request =
            inStore(STORE_NAME)
                .withQuery(TimestampedKeyWithHeadersQuery.<Integer, String>withKey(1))
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecord<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        assertTrue(result.getOnlyPartitionResult().isFailure());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getOnlyPartitionResult().getFailureReason());
    }

    private void startStreams() throws Exception {
        // Caching disabled: every IQv2 query is forced down to the persistent
        // RocksDBTimestampedStoreWithHeaders layer, exercising its KeyQuery handling
        // (rather than being short-circuited by a cache hit).
        startStreams(false);
    }

    private void startStreams(final boolean cachingEnabled) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<TimestampedKeyValueStoreWithHeaders<Integer, String>> storeBuilder =
            Stores.timestampedKeyValueStoreWithHeadersBuilder(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                Serdes.Integer(),
                Serdes.String());
        builder
            .addStateStore(cachingEnabled ? storeBuilder.withCachingEnabled() : storeBuilder.withCachingDisabled())
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new HeadersStoreWriterProcessor(), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.String()));

        kafkaStreams = new KafkaStreams(builder.build(), props());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
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
            store.put(
                record.key(),
                ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
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
