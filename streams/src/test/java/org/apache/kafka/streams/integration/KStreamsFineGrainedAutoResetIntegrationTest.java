/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.kafka.streams.integration;


import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class KStreamsFineGrainedAutoResetIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String TOPIC_A = "topic-A";
    private static final String TOPIC_C = "topic-C";
    private static final String TOPIC_Y = "topic-Y";
    private static final String TOPIC_Z = "topic-Z";

    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private Properties streamsConfiguration;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(TOPIC_1);
        CLUSTER.createTopic(TOPIC_2);
        CLUSTER.createTopic(TOPIC_A);
        CLUSTER.createTopic(TOPIC_C);
        CLUSTER.createTopic(TOPIC_Y);
        CLUSTER.createTopic(TOPIC_Z);
        CLUSTER.createTopic(DEFAULT_OUTPUT_TOPIC);

    }

    @Before
    public void setUp() throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                "testAutoOffsetId",
                CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                props);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldOnlyReadRecordsWhereEarliestSpecified() throws  Exception {
        final Serde<String> stringSerde = Serdes.String();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(StreamsConfig.AutoOffsetReset.EARLIEST, Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Stream = builder.stream(StreamsConfig.AutoOffsetReset.LATEST, Pattern.compile("topic-[A-D]"));
        final KStream<String, String> namedTopicsStream = builder.stream(TOPIC_Y, TOPIC_Z);

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        namedTopicsStream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        final String topic1TestMessage = "topic-1 test";
        final String topic2TestMessage = "topic-2 test";
        final String topicATestMessage = "topic-A test";
        final String topicCTestMessage = "topic-C test";
        final String topicYTestMessage = "topic-Y test";
        final String topicZTestMessage = "topic-Z test";

        IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Collections.singletonList(topic1TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Collections.singletonList(topic2TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Collections.singletonList(topicATestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Collections.singletonList(topicCTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Collections.singletonList(topicYTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Collections.singletonList(topicZTestMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        final List<String> expectedReceivedValues = Arrays.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
        final List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 4);
        final List<String> actualValues = new ArrayList<>(4);

        for (final KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));

    }

}
