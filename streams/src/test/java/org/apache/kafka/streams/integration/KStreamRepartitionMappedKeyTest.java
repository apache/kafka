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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KStreamRepartitionMappedKeyTest {

    @Rule
    public final EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    private final String streamOneInput = "streamOneInput";
    private final String streamTwoInput = "streamTwo";

    private final String outputTopic = "outputTopic";
    private KStreamBuilder builder;
    private Properties streamsConfiguration;


    @Before
    public void whenStarting() throws ExecutionException, InterruptedException {
        cluster.createTopic(streamOneInput, 5, 1);
        cluster.createTopic(streamTwoInput, 5, 1);
        cluster.createTopic(outputTopic);

        builder = new KStreamBuilder();

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


    }

    @Test
    public void shouldMapToDifferentKeyAndJoin() throws ExecutionException, InterruptedException {
        produceMessages();

        final KStream<Long, Integer> streamOne = builder.stream(Serdes.Long(), Serdes.Integer(), streamOneInput);
        final KStream<Integer, String> streamTwo = builder.stream(Serdes.Integer(), Serdes.String(), streamTwoInput);

        streamOne.map(new KeyValueMapper<Long, Integer, KeyValue<Integer, Integer>>() {
            @Override
            public KeyValue<Integer, Integer> apply(final Long key, final Integer value) {
                return new KeyValue<>(value, value);
            }
        }, Serdes.Integer(), Serdes.Integer(), "the-key-mapped-topic")
                .join(streamTwo, new ValueJoiner<Integer, String, String>() {
                    @Override
                    public String apply(final Integer value1, final String value2) {
                        return value1 + ":" + value2;
                    }
                }, JoinWindows.of("the-join").within(60 * 1000), Serdes.Integer(), Serdes.Integer(), Serdes.String())
                .to(Serdes.Integer(), Serdes.String(), outputTopic);


        final KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        final List<String> expected = Arrays.asList("1:A", "2:B", "3:C", "4:D", "5:E");
        final List<String> received = receiveMessages(new StringDeserializer(), 5);
        Collections.sort(received);
        assertThat(received, is(expected));
    }


//    @Test
//    public void shouldSelectKeyAndJoin() throws ExecutionException, InterruptedException {
//        produceMessages();
//        final KStream<Long, Integer> streamOne = builder.stream(Serdes.Long(), Serdes.Integer(), streamOneInput);
//        final KStream<Integer, String> streamTwo = builder.stream(Serdes.Integer(), Serdes.String(), streamTwoInput);
//
//        streamOne.selectKey(new KeyValueMapper<Long, Integer, Integer>() {
//            @Override
//            public Integer apply(final Long key, final Integer value) {
//                return value;
//            }
//        }).join(streamTwo, new ValueJoiner<Integer, String, String>() {
//            @Override
//            public String apply(final Integer value1, final String value2) {
//                return value1 + ":" + value2;
//            }
//        }, JoinWindows.of("the-join").within(60 * 1000), Serdes.Integer(), Serdes.Integer(), Serdes.String())
//                .to(Serdes.Integer(), Serdes.String(), outputTopic);
//
//        final KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
//        kafkaStreams.start();
//
//        final List<String> expected = Arrays.asList("1:A", "2:B", "3:C", "4:D", "5:E");
//        final List<String> received = receiveMessages(new StringDeserializer(), 5);
//        assertThat(received, is(expected));
//    }


    private void produceMessages() throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(streamOneInput,
                Arrays.asList(
                        new KeyValue<>(10L, 1),
                        new KeyValue<>(5L, 2),
                        new KeyValue<>(12L, 3),
                        new KeyValue<>(15L, 4),
                        new KeyValue<>(20L, 5)),
                IntegrationTestUtils.producerConfig(cluster.bootstrapServers(),
                        LongSerializer.class,
                        IntegerSerializer.class,
                        new Properties()));

        IntegrationTestUtils.produceKeyValuesSynchronously(streamTwoInput,
                Arrays.asList(
                        new KeyValue<>(1, "A"),
                        new KeyValue<>(2, "B"),
                        new KeyValue<>(3, "C"),
                        new KeyValue<>(4, "D"),
                        new KeyValue<>(5, "E")),
                IntegrationTestUtils.producerConfig(cluster.bootstrapServers(),
                        IntegerSerializer.class,
                        StringSerializer.class,
                        new Properties()));
    }


    private List<String> receiveMessages(final Deserializer<?> valueDeserializer,
                                         final int numMessages) {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kstream-test");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<Integer, ?>
                consumer =
                new KafkaConsumer<>(consumerProperties, new IntegerDeserializer(), valueDeserializer);
        consumer.subscribe(Collections.singleton(outputTopic));

        final List<String> received = new ArrayList<>();
        final long now = System.currentTimeMillis();
        while (received.size() != numMessages
                && System.currentTimeMillis() - now < TimeUnit.MILLISECONDS
                .convert(1, TimeUnit.MINUTES)) {
            final ConsumerRecords<Integer, ?> records = consumer.poll(10);
            for (final ConsumerRecord<Integer, ?> record : records) {
                received.add(record.value().toString());
            }
        }
        consumer.close();
        return received;
    }


}
