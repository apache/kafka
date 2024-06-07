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
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@Timeout(600)
@Tag("integration")
public class KStreamKStreamIntegrationTest {
    private final static int NUM_BROKERS = 1;

    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String LEFT_STREAM = "leftStream";
    private final static String RIGHT_STREAM = "rightStream";
    private final static String OUTPUT = "output";
    private Properties streamsConfig;
    private KafkaStreams streams;
    private final static Properties CONSUMER_CONFIG = new Properties();
    private final static Properties PRODUCER_CONFIG = new Properties();

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();

        //Use multiple partitions to ensure distribution of keys.
        CLUSTER.createTopic(LEFT_STREAM, 4, 1);
        CLUSTER.createTopic(RIGHT_STREAM, 4, 1);
        CLUSTER.createTopic(OUTPUT, 4, 1);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "result-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws IOException {
        final String stateDirBasePath = TestUtils.tempDirectory().getPath();
        final String safeTestName = safeUniqueTestName(testInfo);
        streamsConfig = getStreamsConfig(safeTestName);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath);
        streamsConfig.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX, 0L);
    }

    @AfterEach
    public void after() throws IOException {
        if (streams != null) {
            streams.close();
            streams = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @Test
    public void shouldOuterJoin() throws Exception {
        final Set<KeyValue<String, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>("Key-1", "value1=left-1a,value2=null"));
        expected.add(new KeyValue<>("Key-2", "value1=left-2a,value2=null"));
        expected.add(new KeyValue<>("Key-3", "value1=left-3a,value2=null"));
        expected.add(new KeyValue<>("Key-4", "value1=left-4a,value2=null"));
        expected.add(new KeyValue<>(null, "value1=left-5a,value2=null"));

        verifyKStreamKStreamOuterJoin(expected);
    }

    private void verifyKStreamKStreamOuterJoin(final Set<KeyValue<String, String>> expectedResult) throws Exception {
        streams = prepareTopology(streamsConfig);

        startApplicationAndWaitUntilRunning(Collections.singletonList(streams), ofSeconds(120));

        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<String, String>> left1 = asList(
                new KeyValue<>("Key-1", "left-1a"),
                new KeyValue<>("Key-2", "left-2a"),
                new KeyValue<>("Key-3", "left-3a"),
                new KeyValue<>("Key-4", "left-4a"),
                new KeyValue<>(null, "left-5a")
        );

        final List<KeyValue<String, String>> left2 = asList(
                new KeyValue<>("Key-1", "left-1b"),
                new KeyValue<>("Key-2", "left-2b"),
                new KeyValue<>("Key-3", "left-3b"),
                new KeyValue<>("Key-4", "left-4b")
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_STREAM, left1, PRODUCER_CONFIG, MOCK_TIME);
        MOCK_TIME.sleep(10000);
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_STREAM, left2, PRODUCER_CONFIG, MOCK_TIME);

        final Set<KeyValue<String, String>> result = new HashSet<>(waitUntilMinKeyValueRecordsReceived(
            CONSUMER_CONFIG,
            OUTPUT,
            expectedResult.size()));

        assertThat(expectedResult, equalTo(result));
    }

    private Properties getStreamsConfig(final String testName) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStream-KStream-join" + testName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        return streamsConfig;
    }

    private static KafkaStreams prepareTopology(final Properties streamsConfig) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream(LEFT_STREAM);
        final KStream<String, String> stream2 = builder.stream(RIGHT_STREAM);

        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;

        stream1.outerJoin(stream2, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(10))).to(OUTPUT);

        return new KafkaStreams(builder.build(streamsConfig), streamsConfig);
    }

}