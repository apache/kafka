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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category(IntegrationTest.class)
public class ErrorHandlingIntegrationTest {

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public TestName testName = new TestName();

    private final String testId = safeUniqueTestName(getClass(), testName);
    private final String appId = "appId_" + testId;
    private final Properties properties = props();

    // Task 0
    private final String inputTopic = "input" + testId;
    private final String outputTopic = "output" + testId;
    // Task 1
    private final String errorInputTopic = "error-input" + testId;
    private final String errorOutputTopic = "error-output" + testId;

    @Before
    public void setup() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, errorInputTopic, errorOutputTopic, inputTopic, outputTopic);
    }

    private Properties props() {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath()),
                mkEntry(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 15000L),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class))
        );
    }

    @Test
    public void shouldBackOffTaskAndEmitDataWithinSameTopology() throws Exception {
        final AtomicInteger noOutputExpected = new AtomicInteger(0);
        final AtomicInteger outputExpected = new AtomicInteger(0);

        try (final KafkaStreamsNamedTopologyWrapper kafkaStreams = new KafkaStreamsNamedTopologyWrapper(properties)) {
            kafkaStreams.setUncaughtExceptionHandler(exception -> StreamThreadExceptionResponse.REPLACE_THREAD);

            final NamedTopologyBuilder builder = kafkaStreams.newNamedTopologyBuilder("topology_A");
            builder.stream(inputTopic).peek((k, v) -> outputExpected.incrementAndGet()).to(outputTopic);
            builder.stream(errorInputTopic)
                .peek((k, v) -> {
                    throw new RuntimeException("Kaboom");
                })
                .peek((k, v) -> noOutputExpected.incrementAndGet())
                .to(errorOutputTopic);

            kafkaStreams.addNamedTopology(builder.build());

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                errorInputTopic,
                Arrays.asList(
                    new KeyValue<>(1, "A")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);
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
            assertThat(noOutputExpected.get(), equalTo(0));
            assertThat(outputExpected.get(), equalTo(2));
        }
    }
}
