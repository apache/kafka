/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that reads data from an input topic and writes the same data as-is to
 * a new output topic, using an embedded Kafka cluster.
 */
public class PassThroughIntegrationTest {
    @ClassRule
    public static EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        cluster.createTopic(DEFAULT_INPUT_TOPIC);
        cluster.createTopic(DEFAULT_OUTPUT_TOPIC);
    }

    @Test
    public void shouldWriteTheInputDataAsIsToTheOutputTopic() throws Exception {
        List<String> inputValues = Arrays.asList(
            "hello world",
            "the world is not enough",
            "the world of the stock market is coming to an end"
        );

        //
        // Step 1: Configure and start the processor topology.
        //
        KStreamBuilder builder = new KStreamBuilder();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pass-through-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zKConnectString());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Write the input data as-is to the output topic.
        builder.stream(DEFAULT_INPUT_TOPIC).to(DEFAULT_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerConfig);

        // Give the stream processing application some time to do its work.
        Thread.sleep(10000);
        streams.close();

        //
        // Step 3: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "pass-through-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> actualValues = IntegrationTestUtils.readValues(DEFAULT_OUTPUT_TOPIC, consumerConfig, inputValues.size());
        assertThat(actualValues).isEqualTo(inputValues);
    }
}