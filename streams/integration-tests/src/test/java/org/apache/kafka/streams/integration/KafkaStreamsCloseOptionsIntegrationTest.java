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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

@Tag("integration")
@Timeout(600)
public class KafkaStreamsCloseOptionsIntegrationTest {
    private static MockTime mockTime;

    protected static final String INPUT_TOPIC = "inputTopic";
    protected static final String OUTPUT_TOPIC = "outputTopic";

    protected Properties streamsConfig;
    protected static KafkaStreams streams;
    protected static Admin adminClient;
    protected Properties commonClientConfig;
    private Properties producerConfig;
    protected Properties resultConsumerConfig;
    private final File testFolder = TestUtils.tempDirectory();

    public static final EmbeddedKafkaCluster CLUSTER;

    static {
        final Properties brokerProps = new Properties();
        brokerProps.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        Utils.closeQuietly(adminClient, "admin");
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testName) throws Exception {
        mockTime = CLUSTER.time;

        final String appID = safeUniqueTestName(testName);

        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        streamsConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "someGroupInstance");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // In this test, we set the SESSION_TIMEOUT_MS_CONFIG high in order to show that the call to
        // `close(CloseOptions)` can remove the application from the Consumder Groups successfully.
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        streamsConfig.putAll(commonClientConfig);

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.putAll(commonClientConfig);

        resultConsumerConfig = new Properties();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        resultConsumerConfig.putAll(commonClientConfig);

        if (adminClient == null) {
            adminClient = Admin.create(commonClientConfig);
        }

        CLUSTER.deleteAllTopics();
        CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, 1);

        add10InputElements();
    }

    @AfterEach
    public void after() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        Utils.delete(testFolder);
    }

    @Test
    public void testCloseOptions() throws Exception {
        // Test with two threads to show that each of the threads is being called to remove clients from the CG.
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close(new CloseOptions().leaveGroup(true).timeout(Duration.ofSeconds(30)));
        waitForEmptyConsumerGroup(adminClient, streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), 0);
    }

    protected Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));
        return builder.build();
    }

    private void add10InputElements() {
        final List<KeyValue<Long, String>> records = Arrays.asList(KeyValue.pair(0L, "aaa"),
            KeyValue.pair(1L, "bbb"),
            KeyValue.pair(0L, "ccc"),
            KeyValue.pair(1L, "ddd"),
            KeyValue.pair(0L, "eee"),
            KeyValue.pair(1L, "fff"),
            KeyValue.pair(0L, "ggg"),
            KeyValue.pair(1L, "hhh"),
            KeyValue.pair(0L, "iii"),
            KeyValue.pair(1L, "jjj"));

        for (final KeyValue<Long, String> record : records) {
            mockTime.sleep(10);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(record), producerConfig, mockTime.milliseconds());
        }
    }
}
