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
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedRangeWithHeadersQuery;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class TimestampedRangeWithHeadersQueryIntegrationTest {

    private static final String STORE_NAME = "headers-store";

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;
    // Accumulated position of the produced input records, so queries can require freshness via
    // PositionBound.at(inputPosition) -- matching the IQv2StoreIntegrationTest convention rather than
    // relying on output-topic consumption as the readiness signal.
    private Position inputPosition;
    private long nextInputOffset;

    private KafkaStreams kafkaStreams;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final Headers HEADERS1 = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "1.0".getBytes());

    private static final Headers HEADERS2 = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "2.0".getBytes());

    private static final Headers EMPTY_HEADERS = new RecordHeaders();

    public TestInfo testInfo;

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
        // single partition so the query has exactly one partition result
        CLUSTER.createTopic(inputStream, 1, 1);
        CLUSTER.createTopic(outputStream, 1, 1);
        baseTimestamp = CLUSTER.time.milliseconds();
        inputPosition = Position.emptyPosition();
        nextInputOffset = 0L;
    }

    @AfterEach
    public void afterTest() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldHandleTimestampedRangeWithHeadersQuery() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                ).withCachingDisabled()
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(HeadersStoreWriterProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props("app-"));
        kafkaStreams.start();

        // Write: keys 1,2 (HEADERS1), key 3 (empty headers), key 4 (then tombstone it).
        produceWithHeaders(baseTimestamp, HEADERS1, KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));
        produceWithHeaders(baseTimestamp + 1, EMPTY_HEADERS, KeyValue.pair(3, "three"));
        produceWithHeaders(baseTimestamp + 2, HEADERS2, KeyValue.pair(4, "four"));
        produceWithHeaders(baseTimestamp + 3, EMPTY_HEADERS, KeyValue.pair(4, null)); // tombstone

        // Full scan, ascending: keys 1, 2, 3 (key 4 tombstoned and omitted).
        final List<ReadOnlyRecord<Integer, String>> ascending = runQuery(TimestampedRangeWithHeadersQuery.withNoBounds());
        assertEquals(List.of(1, 2, 3), keys(ascending));
        assertEquals(List.of("one", "two", "three"), values(ascending));

        // Headers, timestamps, and key are carried on each record.
        assertEquals(HEADERS1, ascending.get(0).headers());
        assertEquals(baseTimestamp, ascending.get(0).timestamp());
        assertEquals(HEADERS1, ascending.get(1).headers());
        // Record written without headers round-trips as empty (never null) headers.
        final Headers emptyHeaders = ascending.get(2).headers();
        assertNotNull(emptyHeaders);
        assertEquals(0, emptyHeaders.toArray().length);
        assertEquals(baseTimestamp + 1, ascending.get(2).timestamp());

        // Returned headers are a read-only snapshot.
        assertTrue(headersAreReadOnly(ascending.get(0).headers()));

        // Full scan, descending.
        final List<ReadOnlyRecord<Integer, String>> descending =
            runQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds().withDescendingKeys());
        assertEquals(List.of(3, 2, 1), keys(descending));

        // Bounded ranges.
        assertEquals(List.of(2, 3), keys(runQuery(TimestampedRangeWithHeadersQuery.withRange(2, 3))));
        assertEquals(List.of(2, 3), keys(runQuery(TimestampedRangeWithHeadersQuery.withLowerBound(2))));
        assertEquals(List.of(1, 2), keys(runQuery(TimestampedRangeWithHeadersQuery.withUpperBound(2))));
    }

    @Test
    public void shouldFailWithUnknownQueryTypeAgainstNonHeadersStore() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                ).withCachingDisabled()
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(NonHeadersStoreWriterProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props("app-"));
        kafkaStreams.start();

        produceWithHeaders(baseTimestamp, EMPTY_HEADERS, KeyValue.pair(1, "one"));

        final StateQueryRequest<ReadOnlyRecordIterator<Integer, String>> request =
            inStore(STORE_NAME).withQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds())
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final Map<Integer, QueryResult<ReadOnlyRecordIterator<Integer, String>>> partitionResults =
            result.getPartitionResults();
        assertFalse(partitionResults.isEmpty());
        for (final QueryResult<ReadOnlyRecordIterator<Integer, String>> partitionResult : partitionResults.values()) {
            assertTrue(partitionResult.isFailure());
            assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, partitionResult.getFailureReason());
        }
    }

    @Test
    public void shouldThrowForPlainSupplierWithNoRepresentableTimestamp() throws Exception {
        // WithHeaders builder over a plain (non-timestamped) supplier: the store cannot persist a
        // timestamp, so entries come back with timestamp = -1, which cannot be a ReadOnlyRecord. The
        // query itself succeeds, but advancing the iterator throws.
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                    Stores.persistentKeyValueStore(STORE_NAME),
                    Serdes.Integer(),
                    Serdes.String()
                ).withCachingDisabled()
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(HeadersStoreWriterProcessor::new, STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props("app-"));
        kafkaStreams.start();

        produceWithHeaders(baseTimestamp, HEADERS1, KeyValue.pair(1, "one"), KeyValue.pair(2, "two"));

        final StateQueryRequest<ReadOnlyRecordIterator<Integer, String>> request =
            inStore(STORE_NAME).withQuery(TimestampedRangeWithHeadersQuery.<Integer, String>withNoBounds())
                .withPositionBound(PositionBound.at(inputPosition));
        final StateQueryResult<ReadOnlyRecordIterator<Integer, String>> result =
            IntegrationTestUtils.iqv2WaitForResult(kafkaStreams, request);

        final QueryResult<ReadOnlyRecordIterator<Integer, String>> partitionResult = result.getOnlyPartitionResult();
        assertTrue(partitionResult.isSuccess());
        try (ReadOnlyRecordIterator<Integer, String> iterator = partitionResult.getResult()) {
            assertThrows(StreamsException.class, iterator::next);
        }
    }

    private List<ReadOnlyRecord<Integer, String>> runQuery(final TimestampedRangeWithHeadersQuery<Integer, String> query) {
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

    private static boolean headersAreReadOnly(final Headers headers) {
        try {
            headers.add("x", new byte[0]);
            return false;
        } catch (final IllegalStateException expected) {
            return true;
        }
    }

    private Properties props(final String prefix) {
        final String safeTestName = safeUniqueTestName(testInfo);
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, prefix + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @SuppressWarnings("varargs")
    @SafeVarargs
    private final void produceWithHeaders(final long timestamp,
                                          final Headers headers,
                                          final KeyValue<Integer, String>... keyValues) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            Arrays.asList(keyValues),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, StringSerializer.class),
            headers,
            timestamp,
            false);
        // Single-partition input topic with contiguous offsets starting at 0: advance to the latest
        // produced offset so PositionBound.at(inputPosition) requires the store to have read this far.
        nextInputOffset += keyValues.length;
        inputPosition.withComponent(inputStream, 0, nextInputOffset - 1);
    }

    /**
     * Writes each incoming record into the headers-aware store (deleting on a null value), then
     * forwards a marker downstream so the test can wait for processing to complete.
     */
    private static class HeadersStoreWriterProcessor implements Processor<Integer, String, Integer, Integer> {
        private ProcessorContext<Integer, Integer> context;
        private TimestampedKeyValueStoreWithHeaders<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            if (record.value() == null) {
                store.delete(record.key());
            } else {
                store.put(record.key(),
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
            }
            context.forward(record.withValue(1));
        }
    }

    /**
     * Writes each incoming record into a plain (non-headers) timestamped store, then forwards a
     * marker downstream so the test can wait for processing to complete.
     */
    private static class NonHeadersStoreWriterProcessor implements Processor<Integer, String, Integer, Integer> {
        private ProcessorContext<Integer, Integer> context;
        private TimestampedKeyValueStore<Integer, String> store;

        @Override
        public void init(final ProcessorContext<Integer, Integer> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<Integer, String> record) {
            if (record.value() == null) {
                store.delete(record.key());
            } else {
                store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()));
            }
            context.forward(record.withValue(1));
        }
    }
}
