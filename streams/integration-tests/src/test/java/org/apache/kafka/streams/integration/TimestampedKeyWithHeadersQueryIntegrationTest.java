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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyWithHeadersQuery;
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
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end PoC test for KIP-1356 {@link TimestampedKeyWithHeadersQuery}.
 *
 * <p>Builds a KIP-1271 {@code WithHeaders} store, writes records (with headers) into it through a
 * processor, then queries the store through IQv2 and asserts that the returned
 * {@link ValueTimestampHeaders} carries value, timestamp, and the exact headers written.
 */
@Tag("integration")
public class TimestampedKeyWithHeadersQueryIntegrationTest {

    private static final String STORE_NAME = "headers-store";

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final Headers HEADERS = new RecordHeaders()
        .add("source", "test".getBytes())
        .add("version", "1.0".getBytes());

    private String inputStream;
    private String outputStream;
    private long baseTimestamp;
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

        // wait until all 4 input records have been processed (one output per input record)
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), IntegerDeserializer.class, StringDeserializer.class),
            outputStream,
            4);

        // key 1: value + timestamp + headers round-trip
        final ValueTimestampHeaders<String> result1 = query(1);
        assertEquals("a0", result1.value());
        assertEquals(baseTimestamp, result1.timestamp());
        assertEquals(HEADERS, result1.headers());

        // key 2: written with no headers -> empty (never null) headers
        final ValueTimestampHeaders<String> result2 = query(2);
        assertEquals("b0", result2.value());
        assertEquals(baseTimestamp + 1, result2.timestamp());
        assertEquals(new RecordHeaders(), result2.headers());

        // key 3: tombstoned -> null result, never a partially-populated wrapper
        assertNull(query(3));

        // never-written key -> null result
        assertNull(query(999));
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
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), IntegerDeserializer.class, StringDeserializer.class),
            outputStream,
            1);

        final StateQueryResult<ValueTimestampHeaders<String>> result =
            kafkaStreams.query(inStore(STORE_NAME).withQuery(TimestampedKeyWithHeadersQuery.withKey(1)));

        assertTrue(result.getOnlyPartitionResult().isFailure());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getOnlyPartitionResult().getFailureReason());
    }

    private void startStreams() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedKeyValueStoreWithHeadersBuilder(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME),
                        Serdes.Integer(),
                        Serdes.String())
                    // Disable caching so every IQv2 query is forced down to the persistent
                    // RocksDBTimestampedStoreWithHeaders layer, exercising its KeyQuery handling
                    // (rather than being short-circuited by a cache hit).
                    .withCachingDisabled())
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new HeadersStoreWriterProcessor(), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.String()));

        kafkaStreams = new KafkaStreams(builder.build(), props());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
    }

    private ValueTimestampHeaders<String> query(final int key) {
        final StateQueryRequest<ValueTimestampHeaders<String>> request =
            inStore(STORE_NAME).withQuery(TimestampedKeyWithHeadersQuery.<Integer, String>withKey(key));
        final StateQueryResult<ValueTimestampHeaders<String>> result = kafkaStreams.query(request);
        // getOnlyPartitionResult() returns null when the single partition result is a successful
        // null (tombstoned / absent key), which we surface to the caller as a null lookup.
        final QueryResult<ValueTimestampHeaders<String>> onlyResult = result.getOnlyPartitionResult();
        return onlyResult == null ? null : onlyResult.getResult();
    }

    private Properties props() {
        final String safeTestName = safeUniqueTestName(testInfo);
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @SuppressWarnings("varargs")
    @SafeVarargs
    private final void produceDataToTopicWithHeaders(final String topic,
                                                     final long timestamp,
                                                     final Headers headers,
                                                     final KeyValue<Integer, String>... keyValues) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            Arrays.asList(keyValues),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, StringSerializer.class),
            headers,
            timestamp,
            false);
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
