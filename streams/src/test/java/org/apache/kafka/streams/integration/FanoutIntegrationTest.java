/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End-to-end integration test that demonstrates "fan-out", using an embedded Kafka cluster.
 *
 * This example shows how you can read from one input topic/stream, transform the data (here:
 * trivially) in two different ways via two intermediate streams, and then write the respective
 * results to two output topics.
 *
 * <pre>
 * {@code
 *
 *                                         +---map()---> stream2 ---to()---> Kafka topic B
 *                                         |
 * Kafka topic A ---stream()--> stream1 ---+
 *                                         |
 *                                         +---map()---> stream3 ---to()---> Kafka topic C
 *
 * }
 * </pre>
 */
public class FanoutIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final String INPUT_TOPIC_A = "A";
    private static final String OUTPUT_TOPIC_B = "B";
    private static final String OUTPUT_TOPIC_C = "C";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC_A);
        CLUSTER.createTopic(OUTPUT_TOPIC_B);
        CLUSTER.createTopic(OUTPUT_TOPIC_C);
    }

    @Test
    public void shouldFanoutTheInput() throws Exception {
        List<String> inputValues = Arrays.asList("Hello", "World");
        List<String> expectedValuesForB = new ArrayList<>();
        List<String> expectedValuesForC = new ArrayList<>();
        for (String input : inputValues) {
            expectedValuesForB.add(input.toUpperCase(Locale.getDefault()));
            expectedValuesForC.add(input.toLowerCase(Locale.getDefault()));
        }

        //
        // Step 1: Configure and start the processor topology.
        //
        KStreamBuilder builder = new KStreamBuilder();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<byte[], String> stream1 = builder.stream(INPUT_TOPIC_A);
        KStream<byte[], String> stream2 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(String value) {
                    return value.toUpperCase(Locale.getDefault());
                }
            });
        KStream<byte[], String> stream3 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(String value) {
                    return value.toLowerCase(Locale.getDefault());
                }
            });
        stream2.to(OUTPUT_TOPIC_B);
        stream3.to(OUTPUT_TOPIC_C);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(INPUT_TOPIC_A, inputValues, producerConfig);

        // Give the stream processing application some time to do its work.
        Thread.sleep(5000);
        streams.close();

        //
        // Step 3: Verify the application's output data.
        //

        // Verify output topic B
        Properties consumerConfigB = new Properties();
        consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-integration-test-standard-consumer-topicB");
        consumerConfigB.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> actualValuesForB = IntegrationTestUtils.readValues(OUTPUT_TOPIC_B, consumerConfigB, inputValues.size());
        assertThat(actualValuesForB, equalTo(expectedValuesForB));

        // Verify output topic C
        Properties consumerConfigC = new Properties();
        consumerConfigC.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigC.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-integration-test-standard-consumer-topicC");
        consumerConfigC.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigC.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfigC.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> actualValuesForC = IntegrationTestUtils.readValues(OUTPUT_TOPIC_C, consumerConfigC, inputValues.size());
        assertThat(actualValuesForC, equalTo(expectedValuesForC));
    }

}