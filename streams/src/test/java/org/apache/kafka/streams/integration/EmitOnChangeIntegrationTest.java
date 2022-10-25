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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

@Timeout(600)
@Tag("integration")
public class EmitOnChangeIntegrationTest {
    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static String inputTopic;
    private static String inputTopic2;
    private static String outputTopic;
    private static String outputTopic2;
    private static String appId = "";

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        final String testId = safeUniqueTestName(getClass(), testInfo);
        appId = "appId_" + testId;
        inputTopic = "input" + testId;
        inputTopic2 = "input2" + testId;
        outputTopic = "output" + testId;
        outputTopic2 = "output2" + testId;
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic, outputTopic, inputTopic2, outputTopic2);
    }

    @Test
    public void shouldEmitSameRecordAfterFailover() throws Exception {
        final Properties properties  = mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1),
                mkEntry(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 300000L),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000)
            )
        );

        final AtomicBoolean shouldThrow = new AtomicBoolean(true);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Materialized.as("test-store"))
            .toStream()
            .map((key, value) -> {
                if (shouldThrow.compareAndSet(true, false)) {
                    throw new IllegalStateException("Kaboom");
                } else {
                    return new KeyValue<>(key, value);
                }
            })
            .to(outputTopic);
        builder.stream(inputTopic2).to(outputTopic2);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler(exception -> StreamThreadExceptionResponse.REPLACE_THREAD);
            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);

            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);

            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic2,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);

            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                outputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                outputTopic2,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
        }
    }
}
