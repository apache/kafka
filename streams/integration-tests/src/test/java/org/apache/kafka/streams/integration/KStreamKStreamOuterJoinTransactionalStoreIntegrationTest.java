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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Reproduces a bug KAFKA-20749: a KStream-KStream OUTER join crashes when it
 * emits non-joined (outer) records under {@code exactly_once_v2} <em>and</em>
 * {@code enable.transactional.statestores=true}. Reading the outer-join buffer
 * ({@link org.apache.kafka.streams.state.internals.ListValueStore}) throws a {@code SerializationException}
 * because the transactional persistent-store value round-trip does not preserve the exact bytes that
 * {@code ListValueStore} wrote with {@code Serdes.ListSerde(ArrayList.class, Serdes.ByteArray())}.
 *
 * <p>The test is parameterized over {@code enable.transactional.statestores}: the {@code false} case is a
 * control that passes today (same topology, non-transactional stores), while the {@code true} case fails on
 * current trunk (the {@code SerializationException} kills the StreamThread, so the outer result is never
 * emitted) and passes once the bug is fixed.
 */
@Timeout(600)
@Tag("integration")
public class KStreamKStreamOuterJoinTransactionalStoreIntegrationTest {

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
        applicationId = "outer-join-txn-store-" + safeUniqueTestName(testInfo);
        leftTopic = applicationId + "-left";
        rightTopic = applicationId + "-right";
        outputTopic = applicationId + "-output";
        CLUSTER.createTopic(leftTopic, 1, 1);
        CLUSTER.createTopic(rightTopic, 1, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Disable the store cache so emitted records are not buffered/absorbed by the cache.
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        // Emit non-joined outer records on every stream-time advance (no wall-clock throttle), so a single
        // window-advancing record deterministically triggers emitNonJoinedOuterRecords over the buffer.
        streamsConfig.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX, 0L);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @AfterEach
    public void after() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        CLUSTER.deleteAllTopics();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldEmitOuterRecordWithTransactionalStateStores(final boolean transactionalStateStores) throws Exception {
        streamsConfig.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, transactionalStateStores);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> leftStream = builder.stream(leftTopic);
        final KStream<String, String> rightStream = builder.stream(rightTopic);

        // Default (persistent RocksDB) stores; with enable.transactional.statestores=true the outer-join
        // ListValueStore is wrapped by the transactional accessor — the path that corrupts the value round-trip.
        leftStream.outerJoin(
            rightStream,
            (leftValue, rightValue) -> leftValue + "/" + rightValue,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        ).to(outputTopic);

        streams = new KafkaStreams(builder.build(), streamsConfig);

        final AtomicReference<Throwable> uncaught = new AtomicReference<>();
        streams.setUncaughtExceptionHandler(throwable -> {
            uncaught.compareAndSet(null, throwable);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        startApplicationAndWaitUntilRunning(streams);

        // Unmatched left record -> goes into the outer-join buffer (ListValueStore), no matching right.
        produceRecord(leftTopic, "A", "L1", 1000L);

        // Advance stream time well past the 60s window so emitNonJoinedOuterRecords() runs and reads the
        // buffer. This is where the SerializationException is thrown when stores are transactional.
        produceRecord(leftTopic, "B", "L2", 62000L);

        final List<KeyValue<String, String>> results;
        try {
            results = waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 1, 60000);
        } catch (final AssertionError timeout) {
            // Most informative failure: surface the StreamThread crash that prevented the emit.
            final Throwable crash = uncaught.get();
            if (crash != null) {
                throw new AssertionError(
                    "emitNonJoinedOuterRecords failed to emit the outer record; StreamThread crashed with:\n"
                        + stackTraceToString(crash), crash);
            }
            throw timeout;
        }

        assertNull(uncaught.get(),
            uncaught.get() == null ? "" : "StreamThread crashed:\n" + stackTraceToString(uncaught.get()));

        final KeyValue<String, String> outerResult = results.stream()
            .filter(kv -> "A".equals(kv.key))
            .findFirst()
            .orElse(null);

        assertEquals(KeyValue.pair("A", "L1/null"), outerResult,
            "The unmatched left record A should be emitted as the outer result (A, \"L1/null\")");
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

    private static String stackTraceToString(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
