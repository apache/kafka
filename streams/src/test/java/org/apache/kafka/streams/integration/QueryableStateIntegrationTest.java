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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class QueryableStateIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER =
        new EmbeddedSingleNodeKafkaCluster();
    private static final String STREAM_ONE = "stream-one";
    private static final String STREAM_TWO = "stream-two";
    private static final String OUTPUT_TOPIC = "output";

    private Properties streamsConfiguration;
    private KStreamBuilder builder;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValue<String, String>> stringComparator;
    private Comparator<KeyValue<String, Long>> stringLongComparator;

    @BeforeClass
    public static void createTopics() {
        CLUSTER.createTopic(STREAM_ONE);
        CLUSTER.createTopic(STREAM_TWO);
        CLUSTER.createTopic(OUTPUT_TOPIC);
    }

    @Before
    public void before() throws IOException {
        builder = new KStreamBuilder();
        streamsConfiguration = new Properties();
        final String applicationId = "queryable-state";

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,
                                 TestUtils.tempDirectory("qs-test")
                                     .getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration
            .put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        stringComparator = new Comparator<KeyValue<String, String>>() {

            @Override
            public int compare(final KeyValue<String, String> o1,
                               final KeyValue<String, String> o2) {
                return o1.key.compareTo(o2.key);
            }
        };
        stringLongComparator = new Comparator<KeyValue<String, Long>>() {

            @Override
            public int compare(final KeyValue<String, Long> o1,
                               final KeyValue<String, Long> o2) {
                return o1.key.compareTo(o2.key);
            }
        };
    }

    @After
    public void shutdown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldBeAbleToQueryState() throws Exception {
        final String[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};

        final Set<KeyValue<String, String>> expectedJoinThis = new TreeSet<>(stringComparator);
        expectedJoinThis.addAll(Arrays.asList(
            new KeyValue<>(keys[0], "hello"),
            new KeyValue<>(keys[1], "goodbye"),
            new KeyValue<>(keys[2], "welcome"),
            new KeyValue<>(keys[3], "go"),
            new KeyValue<>(keys[4], "kafka")));

        final Set<KeyValue<String, String>> expectedJoinOther = new TreeSet<>(stringComparator);
        expectedJoinOther.addAll(Arrays.asList(
            new KeyValue<>(keys[0], "world"),
            new KeyValue<>(keys[1], "joanne"),
            new KeyValue<>(keys[2], "home"),
            new KeyValue<>(keys[3], "away"),
            new KeyValue<>(keys[4], "streams")));

        final Set<KeyValue<String, Long>> expectedCount = new TreeSet<>(stringLongComparator);
        for (String key : keys) {
            expectedCount.add(new KeyValue<>(key, 1L));
        }

        IntegrationTestUtils.produceKeyValuesSynchronously(
            STREAM_ONE,
            expectedJoinThis,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()));
        IntegrationTestUtils.produceKeyValuesSynchronously(
            STREAM_TWO,
            expectedJoinOther,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()));

        final KStream<String, String>
            s1 =
            builder.stream(STREAM_ONE);
        final KStream<String, String>
            s2 =
            builder.stream(STREAM_TWO);

        s1.join(s2, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + value2;
            }
        }, JoinWindows.of("join", 10 * 60 * 1000L))
            .to(OUTPUT_TOPIC);

        // Non Windowed
        s1.groupByKey().count("my-count");

        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();

        waitUntilAtLeastOneRecordProcessed();

        final ReadOnlyWindowStore<String, String>
            joinThis =
            kafkaStreams.store("join-this", QueryableStoreTypes.<String, String>windowStore());

        final ReadOnlyWindowStore<String, String>
            joinOther =
            kafkaStreams.store("join-other", QueryableStoreTypes.<String, String>windowStore());

        final ReadOnlyKeyValueStore<String, Long>
            myCount = kafkaStreams.store("my-count", QueryableStoreTypes.<String, Long>keyValueStore());

        verifyCanGetByKey(keys, expectedJoinThis,
                          expectedJoinOther,
                          expectedCount,
                          joinThis,
                          joinOther,
                          myCount);

        verifyRangeAndAll(expectedCount, myCount);

    }

    private void verifyRangeAndAll(final Set<KeyValue<String, Long>> expectedCount,
                                   final ReadOnlyKeyValueStore<String, Long> myCount) {
        final Set<KeyValue<String, Long>> countRangeResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>> countAllResults = new TreeSet<>(stringLongComparator);
        final Set<KeyValue<String, Long>>
            expectedRangeResults =
            new TreeSet<>(stringLongComparator);

        expectedRangeResults.addAll(Arrays.asList(
            new KeyValue<>("hello", 1L),
            new KeyValue<>("go", 1L),
            new KeyValue<>("goodbye", 1L),
            new KeyValue<>("kafka", 1L)
        ));

        try (final KeyValueIterator<String, Long> range = myCount.range("go", "kafka")) {
            while (range.hasNext()) {
                countRangeResults.add(range.next());
            }
        }

        try (final KeyValueIterator<String, Long> all = myCount.all()) {
            while (all.hasNext()) {
                countAllResults.add(all.next());
            }
        }

        assertThat(countRangeResults, equalTo(expectedRangeResults));
        assertThat(countAllResults, equalTo(expectedCount));
    }

    private void verifyCanGetByKey(final String[] keys,
                                   final Set<KeyValue<String, String>> expectedJoinThis,
                                   final Set<KeyValue<String, String>> expectedJoinOther,
                                   final Set<KeyValue<String, Long>> expectedCount,
                                   final ReadOnlyWindowStore<String, String> joinThis,
                                   final ReadOnlyWindowStore<String, String> joinOther,
                                   final ReadOnlyKeyValueStore<String, Long> myCount)
        throws InterruptedException {
        final Set<KeyValue<String, String>> joinThisState = new TreeSet<>(stringComparator);
        final Set<KeyValue<String, String>> joinOtherState = new TreeSet<>(stringComparator);
        final Set<KeyValue<String, Long>> countState = new TreeSet<>(stringLongComparator);

        final long timeout = System.currentTimeMillis() + 30000;
        while (joinThisState.size() < 5 &&
               joinOtherState.size() < 5 &&
               countState.size() < 5 &&
               System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
            for (String key : keys) {
                joinThisState.addAll(fetch(joinThis, key));
                joinOtherState.addAll(fetch(joinOther, key));
                final Long value = myCount.get(key);
                if (value != null) {
                    countState.add(new KeyValue<>(key, value));
                }
            }
        }
        assertThat(joinThisState, equalTo(expectedJoinThis));
        assertThat(joinOtherState, equalTo(expectedJoinOther));
        assertThat(countState, equalTo(expectedCount));
    }

    private void waitUntilAtLeastOneRecordProcessed() throws InterruptedException {
        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                           StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                           StringDeserializer.class.getName());
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(config,
                                                               OUTPUT_TOPIC,
                                                               1,
                                                               60 *
                                                               1000);
    }

    private Set<KeyValue<String, String>> fetch(final ReadOnlyWindowStore<String, String> store,
                                                final String key) {

        final WindowStoreIterator<String> fetch = store.fetch(key, 0, System.currentTimeMillis());
        if (fetch.hasNext()) {
            KeyValue<Long, String> next = fetch.next();
            return Collections.singleton(KeyValue.pair(key, next.value));
        }
        return Collections.emptySet();
    }


}
