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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for verifying ListValueStore deserialization behavior after state restoration
 * in header-aware and default stores used by outer join operations.
 */
@Timeout(600)
@Tag("integration")
public class OuterJoinListValueStoreRestorationTest {

    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private String applicationId;
    private String leftTopic;
    private String rightTopic;
    private String outputTopic;
    private Properties streamsConfig;
    private KafkaStreams streams;

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        applicationId = "outer-join-restoration-test-" + safeUniqueTestName(testInfo);
        leftTopic = applicationId + "-left";
        rightTopic = applicationId + "-right";
        outputTopic = applicationId + "-output";
        CLUSTER.createTopic(leftTopic, 1, 1);
        CLUSTER.createTopic(rightTopic, 1, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);
        streamsConfig = getStreamsConfig();
    }

    @AfterEach
    public void after() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        CLUSTER.deleteAllTopics();
    }

    private static Stream<Arguments> processingGuaranteeAndStoreFormat() {
        return Stream.of(
            Arguments.of(StreamsConfig.EXACTLY_ONCE_V2, StreamsConfig.DSL_STORE_FORMAT_DEFAULT),
            Arguments.of(StreamsConfig.EXACTLY_ONCE_V2, StreamsConfig.DSL_STORE_FORMAT_HEADERS),
            Arguments.of(StreamsConfig.AT_LEAST_ONCE, StreamsConfig.DSL_STORE_FORMAT_DEFAULT),
            Arguments.of(StreamsConfig.AT_LEAST_ONCE, StreamsConfig.DSL_STORE_FORMAT_HEADERS)
        );
    }

    private Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private KafkaStreams createOuterJoinTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> leftStream = builder.stream(leftTopic);
        final KStream<String, String> rightStream = builder.stream(rightTopic);

        leftStream.outerJoin(
            rightStream,
            (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        ).to(outputTopic);

        return new KafkaStreams(builder.build(), streamsConfig);
    }

    @ParameterizedTest
    @MethodSource("processingGuaranteeAndStoreFormat")
    public void testOuterJoinRestorationWithMultipleRecords(final String processingGuarantee,
                                                            final String storeFormat) throws Exception {
        // Configure processing guarantee and store format
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfig.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);

        // Step 1: Initial Topology Start
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        // Step 2: Create Non-Joined Records

        // Produce multiple records to left topic only (no match → non-joined records)
        // CRITICAL: Do NOT advance window yet! Records must stay in store before restoration.
        long timestamp = 1000L;
        for (int i = 0; i < 10; i++) {
            final String key = "key" + i;
            produceRecord(leftTopic, key, "left-" + i, timestamp);
            timestamp += 100;
        }


        // Wait for processing and commit to changelog

        // 1- Use a probe record to verify end-to-end: process + commit
        produceRecord(leftTopic, "probe", "probe-left", timestamp);
        produceRecord(rightTopic, "probe", "probe-right", timestamp);
        // 2- Wait for the join result - this proves processing happened
        waitUntilMinKeyValueRecordsReceived(
            getConsumerConfig(),
            outputTopic,
            1,
            30000
        );
        // 3- Wait for all records to be processed and committed (zero lag)
        // This ensures changelog commits have completed before we close
        waitForCompletion(streams, 2, 30000);

        // Step 3: Force State Restoration
        streams.close(Duration.ofSeconds(30));
        purgeLocalStreamsState(streamsConfig);

        // Step 4: Restart with Restoration
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        // Step 5: Trigger Window Advancement

        // NOW advance window to trigger emitNonJoinedOuterRecords()
        final long timestampBeyondWindow = 62000L; // Beyond 60-second window
        produceRecord(leftTopic, "trigger", "trigger-value", timestampBeyondWindow);

        final List<KeyValue<String, String>> results = waitUntilMinKeyValueRecordsReceived(
            getConsumerConfig(),
            outputTopic,
            10,
            30000
        );

        final Set<String> expectedKeys = IntStream.range(0, 10)
            .mapToObj(i -> "key" + i)
            .collect(Collectors.toSet());

        final Set<String> unmatchedKeys = results.stream()
            .filter(kv -> kv.value != null && kv.value.endsWith("right=null"))
            .map(kv -> kv.key)
            .collect(Collectors.toSet());

        // assert based on record shape
        assertEquals(expectedKeys, unmatchedKeys,
            "All 10 unmatched left records should be emitted after restoration with right=null shape");

        final Set<String> nonProbeKeys = results.stream()
            .filter(kv -> !"probe".equals(kv.key))
            .map(kv -> kv.key)
            .collect(Collectors.toSet());

        // assert based on keys
        assertEquals(expectedKeys, nonProbeKeys,
            "No unexpected keys should appear on the output topic after restoration");
    }

    /**
     * Verifies that per-record headers stored in the header-aware outer-join store survive a full
     * fault-tolerance cycle: the headers are written into the changelog, the local state is wiped,
     * and after the store is rebuilt from the changelog the non-joined records are emitted carrying
     * their original headers.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void testOuterJoinHeadersSurviveRestoration(final String processingGuarantee) throws Exception {
        // Headers are only stored in the header-aware store format.
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfig.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);

        // Step 1: Initial Topology Start
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        // Step 2: Produce non-joined left records, each carrying a distinct header.
        // CRITICAL: Do NOT advance the window yet, so the records (and their headers) stay in the store.
        final Map<String, String> expectedHeaders = IntStream.range(0, 10).boxed()
            .collect(Collectors.toMap(i -> "key" + i, i -> "v" + i));
        long timestamp = 1000L;
        for (int i = 0; i < 10; i++) {
            final Headers headers = new RecordHeaders().add("h", ("v" + i).getBytes(StandardCharsets.UTF_8));
            produceRecordWithHeaders(leftTopic, "key" + i, "left-" + i, headers, timestamp);
            timestamp += 100;
        }

        // Wait for processing and commit to the changelog, proven via a probe join result.
        produceRecord(leftTopic, "probe", "probe-left", timestamp);
        produceRecord(rightTopic, "probe", "probe-right", timestamp);
        waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 1, 30000);
        waitForCompletion(streams, 2, 30000);

        // Step 3: Force State Restoration (wipe local state, rebuild from changelog).
        streams.close(Duration.ofSeconds(30));
        purgeLocalStreamsState(streamsConfig);

        // Step 4: Restart with Restoration
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        // Step 5: Advance the window to trigger emitNonJoinedOuterRecords() for the restored records.
        produceRecord(leftTopic, "trigger", "trigger-value", 62000L); // beyond the 60-second window

        final List<ConsumerRecord<String, String>> results =
            waitUntilMinRecordsReceived(getConsumerConfig(), outputTopic, 10, 30000);

        // Step 6: Each restored non-joined left record must be emitted with its original header.
        final Map<String, String> actualHeaders = results.stream()
            .filter(r -> r.value() != null && r.value().endsWith("right=null"))
            .filter(r -> r.key().startsWith("key"))
            .collect(Collectors.toMap(ConsumerRecord::key, r -> headerValue(r, "h"), (a, b) -> a));

        assertEquals(expectedHeaders, actualHeaders,
            "Each non-joined left record should retain its original header after wipe + changelog restoration");
    }

    /**
     * The downgrade direction: state is written by a HEADERS-format store and then restored by a PLAIN
     * one. This is what the changelog encoding exists for. If the local
     * {@code [headersSize][headers][flag][value]} element format reached the changelog, the PLAIN reader
     * would consume each element's leading empty-headers {@code 0x00} as the {@code LeftOrRightValue}
     * flag and silently emit left records as right ones -- i.e. {@code (null, x)} instead of
     * {@code (x, null)} -- with no exception to signal it.
     * <p>
     * Note this needs no version downgrade: flipping {@code dsl.store.format} back to PLAIN on one
     * version is enough, and it is also the path the "delete the local state and rebuild the store from
     * the changelog" advice in the {@code RocksDBStore} downgrade guard takes.
     */
    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void testHeadersStoreChangelogIsReadableByAPlainStore(final String processingGuarantee) throws Exception {
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfig.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);

        // Step 1: run in HEADERS mode and park non-joined left records (with headers) in the store.
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        long timestamp = 1000L;
        for (int i = 0; i < 10; i++) {
            final Headers headers = new RecordHeaders().add("h", ("v" + i).getBytes(StandardCharsets.UTF_8));
            produceRecordWithHeaders(leftTopic, "key" + i, "left-" + i, headers, timestamp);
            timestamp += 100;
        }

        produceRecord(leftTopic, "probe", "probe-left", timestamp);
        produceRecord(rightTopic, "probe", "probe-right", timestamp);
        waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 1, 30000);
        waitForCompletion(streams, 2, 30000);

        // Step 2: wipe the local state, so the only surviving copy is the changelog.
        streams.close(Duration.ofSeconds(30));
        purgeLocalStreamsState(streamsConfig);

        // Step 3: come back as a PLAIN store -- the downgrade.
        streamsConfig.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_DEFAULT);
        streams = createOuterJoinTopology();
        startApplicationAndWaitUntilRunning(streams);

        // Step 4: advance the window to emit the restored non-joined records.
        produceRecord(leftTopic, "trigger", "trigger-value", 62000L);

        final List<KeyValue<String, String>> results =
            waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 10, 30000);

        final Set<String> expectedKeys = IntStream.range(0, 10)
            .mapToObj(i -> "key" + i)
            .collect(Collectors.toSet());

        // The join side must survive: every restored record is a LEFT record, so it is emitted as
        // "left=<value>, right=null". Under the old changelog format these came back as right records.
        final Set<String> emittedAsLeft = results.stream()
            .filter(kv -> kv.value != null && kv.value.endsWith("right=null"))
            .map(kv -> kv.key)
            .collect(Collectors.toSet());
        assertEquals(expectedKeys, emittedAsLeft,
            "A PLAIN store restoring a HEADERS-written changelog must still see these as left records");

        // The values must survive too: the old format left a stray NUL prepended to each value.
        final Map<String, String> emittedValues = results.stream()
            .filter(kv -> kv.key.startsWith("key"))
            .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value, (a, b) -> a));
        final Map<String, String> expectedValues = IntStream.range(0, 10).boxed()
            .collect(Collectors.toMap(i -> "key" + i, i -> "left=left-" + i + ", right=null"));
        assertEquals(expectedValues, emittedValues,
            "Values must be byte-identical after the downgrade, with no stray prefix");
    }

    private void produceRecordWithHeaders(final String topic,
                                          final String key,
                                          final String value,
                                          final Headers headers,
                                          final long timestamp) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            List.of(new KeyValue<>(key, value)),
            producerConfig,
            headers,
            timestamp,
            false
        );
    }

    private static String headerValue(final ConsumerRecord<?, ?> record, final String key) {
        final Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private void produceRecord(final String topic, final String key, final String value, final long timestamp) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            List.of(new KeyValue<>(key, value)),
            producerConfig,
            timestamp
        );
    }

    private Properties getConsumerConfig() {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + applicationId);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerConfig;
    }
}