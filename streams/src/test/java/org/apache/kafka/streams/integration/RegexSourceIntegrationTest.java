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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
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

/**
 * End-to-end integration test based on using regex and named topics for creating sources, using
 * an embedded Kafka cluster.
 */

public class RegexSourceIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String TOPIC_3 = "topic-3";
    private static final String TOPIC_A = "topic-A";
    private static final String TOPIC_C = "topic-C";
    private static final String TOPIC_Y = "topic-Y";
    private static final String TOPIC_Z = "topic-Z";
    private static final String FA_TOPIC = "fa";
    private static final String FOO_TOPIC = "foo";

    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(TOPIC_1);
        CLUSTER.createTopic(TOPIC_2);
        CLUSTER.createTopic(TOPIC_A);
        CLUSTER.createTopic(TOPIC_C);
        CLUSTER.createTopic(TOPIC_Y);
        CLUSTER.createTopic(TOPIC_Z);
        CLUSTER.createTopic(FA_TOPIC);
        CLUSTER.createTopic(FOO_TOPIC);

    }

    @Test
    public void testShouldSubscribeToNewMatchingTopicsAfterStart() throws Exception {

        String topic1TestMessage = "topic-1 test";
        String topic2TestMessage = "topic-2 test";
        String topic3TestMessage = "topic-3 test";

        final Serde<String> stringSerde = Serdes.String();

        Properties streamsConfiguration = getStreamsConfig();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        //create topic after start will get picked up
        CLUSTER.createTopic(TOPIC_3);

        //pause for metadata discovery
        Thread.sleep(5000);
        Properties producerConfig = getProducerConfig();
        produceMessage(TOPIC_1, Arrays.asList(topic1TestMessage), producerConfig);
        produceMessage(TOPIC_2, Arrays.asList(topic2TestMessage), producerConfig);
        produceMessage(TOPIC_3, Arrays.asList(topic3TestMessage), producerConfig);


        Properties consumerConfig = getConsumerConfig();
        List<String> expectedReceivedValues = Arrays.asList(topic1TestMessage, topic2TestMessage, topic3TestMessage);
        List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 3);
        List<String> actualValues = new ArrayList<>(3);

        for (KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
        CLUSTER.deleteTopic(TOPIC_3);
    }


    @Test
    public void testShouldReadFromRegexAndNamedTopics() throws Exception {

        String topic1TestMessage = "topic-1 test";
        String topic2TestMessage = "topic-2 test";
        String topicATestMessage = "topic-A test";
        String topicCTestMessage = "topic-C test";
        String topicYTestMessage = "topic-Y test";
        String topicZTestMessage = "topic-Z test";


        final Serde<String> stringSerde = Serdes.String();

        Properties streamsConfiguration = getStreamsConfig();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d"));
        KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("topic-[A-D]"));
        KStream<String, String> namedTopicsStream = builder.stream(TOPIC_Y, TOPIC_Z);

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        namedTopicsStream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Properties producerConfig = getProducerConfig();

        produceMessage(TOPIC_1, Arrays.asList(topic1TestMessage), producerConfig);
        produceMessage(TOPIC_2, Arrays.asList(topic2TestMessage), producerConfig);
        produceMessage(TOPIC_A, Arrays.asList(topicATestMessage), producerConfig);
        produceMessage(TOPIC_C, Arrays.asList(topicCTestMessage), producerConfig);
        produceMessage(TOPIC_Y, Arrays.asList(topicYTestMessage), producerConfig);
        produceMessage(TOPIC_Z, Arrays.asList(topicZTestMessage), producerConfig);

        Properties consumerConfig = getConsumerConfig();

        List<String> expectedReceivedValues = Arrays.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
        List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 6);
        List<String> actualValues = new ArrayList<>(6);

        for (KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
    }

    @Test(expected = AssertionError.class)
    public void testNoMessagesSentExceptionFromOverlappingPatterns() throws Exception {

        String fooMessage = "fooMessage";
        String fMessage = "fMessage";


        final Serde<String> stringSerde = Serdes.String();

        Properties streamsConfiguration = getStreamsConfig();

        KStreamBuilder builder = new KStreamBuilder();

        /*
          overlapping patterns here, no messages should be sent as TopologyBuilderException
          will be thrown when the processor topology is built.
         */
        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("foo.*"));
        KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("f.*"));


        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);


        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Properties producerConfig = getProducerConfig();

        produceMessage(FA_TOPIC, Arrays.asList(fMessage), producerConfig);
        produceMessage(FOO_TOPIC, Arrays.asList(fooMessage), producerConfig);

        Properties consumerConfig = getConsumerConfig();

        try {
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 2, 5000);
        } finally {
            streams.close();
        }

    }

    private void produceMessage(String inputTopic, List<String> input, Properties producerConfig) throws Exception {
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, input, producerConfig);
    }


    private Properties getProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerConfig;
    }

    private Properties getStreamsConfig() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "regex-source-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "500");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return streamsConfiguration;
    }

    private Properties getConsumerConfig() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "regex-source-integration-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return consumerConfig;
    }


}
