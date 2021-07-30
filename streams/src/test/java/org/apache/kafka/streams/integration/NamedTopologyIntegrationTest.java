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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.TestUtils;

import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class NamedTopologyIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private final static String INPUT_STREAM_1 = "input-stream-1";
    private final static String INPUT_STREAM_2 = "input-stream-2";
    private final static String INPUT_STREAM_3 = "input-stream-3";
    private final static String OUTPUT_STREAM_1 = "output-stream-1";
    private final static String OUTPUT_STREAM_2 = "output-stream-2";
    private final static String OUTPUT_STREAM_3 = "output-stream-3";

    private static Properties producerConfig;
    private static Properties consumerConfig;

    @BeforeClass
    public static void initializeClusterAndStandardTopics() throws Exception {
        CLUSTER.start();

        CLUSTER.createTopic(INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_3, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_3, 2, 1);

        producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, LongSerializer.class);
        consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);

        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_3, STANDARD_INPUT_DATA);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TestName testName = new TestName();
    private String appId;
    private String changelog1;
    private String changelog2;
    private String changelog3;

    final static List<KeyValue<String, Long>> STANDARD_INPUT_DATA = asList(KeyValue.pair("A", 100L), KeyValue.pair("B", 200L), KeyValue.pair("A", 300L), KeyValue.pair("C", 400L));
    final static List<KeyValue<String, Long>> STANDARD_OUTPUT_DATA = asList(KeyValue.pair("B", 1L), KeyValue.pair("A", 2L), KeyValue.pair("C", 1L)); // output of basic count topology with caching

    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final NamedTopologyStreamsBuilder topology1Builder = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder topology2Builder = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder topology3Builder = new NamedTopologyStreamsBuilder("topology-3");

    Properties props;
    KafkaStreamsNamedTopologyWrapper streams;

    private Properties configProps() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @Before
    public void setup() {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        props = configProps();
        changelog1 = appId + "-topology-1-store-changelog";
        changelog2 = appId + "-topology-2-store-changelog";
        changelog3 = appId + "-topology-3-store-changelog";
    }

    @After
    public void shutdown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
    }

    @Test
    public void shouldPrefixAllInternalTopicNamesWithNamedTopology() throws Exception {
        final String countTopologyName = "count-topology";
        final String fkjTopologyName = "FKJ-topology";

        final NamedTopologyStreamsBuilder countBuilder = new NamedTopologyStreamsBuilder(countTopologyName);
        countBuilder.stream(INPUT_STREAM_1).selectKey((k, v) -> k).groupByKey().count();

        final NamedTopologyStreamsBuilder fkjBuilder = new NamedTopologyStreamsBuilder(fkjTopologyName);

        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final KTable<String, Long> left = fkjBuilder.table(
            INPUT_STREAM_2,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), props, true),
                serdeScope.decorateSerde(Serdes.Long(), props, false))
        );
        final KTable<String, Long> right = fkjBuilder.table(
            INPUT_STREAM_3,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), props, true),
                serdeScope.decorateSerde(Serdes.Long(), props, false))
        );
        left.join(
            right,
            Object::toString,
            (value1, value2) -> String.valueOf(value1 + value2),
            Materialized.with(null, serdeScope.decorateSerde(Serdes.String(), props, false)));

        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(fkjBuilder, countBuilder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        final String countTopicPrefix = appId + "-" + countTopologyName;
        final String fkjTopicPrefix = appId + "-" + fkjTopologyName;
        final  Set<String> internalTopics = CLUSTER
            .getAllTopicsInCluster().stream()
            .filter(t -> t.endsWith("-repartition") || t.endsWith("-changelog") || t.endsWith("-topic"))
            .collect(Collectors.toSet());
        assertThat(internalTopics, is(mkSet(
            countTopicPrefix + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition",
            countTopicPrefix + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-changelog",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-0000000010-changelog",
            fkjTopicPrefix + "-" + INPUT_STREAM_2 + "-STATE-STORE-0000000000-changelog",
            fkjTopicPrefix + "-" + INPUT_STREAM_3 + "-STATE-STORE-0000000003-changelog"))
        );
    }

    @Test
    public void shouldProcessSingleNamedTopologyAndPrefixInternalTopics() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1)
            .selectKey((k, v) -> k)
            .groupByKey()
            .count(Materialized.as(Stores.persistentKeyValueStore("store")))
            .toStream().to(OUTPUT_STREAM_1);
        streams = new KafkaStreamsNamedTopologyWrapper(topology1Builder.buildNamedTopology(props), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
        final List<KeyValue<String, Long>> results = waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3);
        assertThat(results, equalTo(STANDARD_OUTPUT_DATA));

        final Set<String> allTopics = CLUSTER.getAllTopicsInCluster();
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-changelog"), is(true));
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-repartition"), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryAndPersistentStateStores() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(INPUT_STREAM_3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(STANDARD_OUTPUT_DATA));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(changelog1, changelog2, changelog3)), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryStateStores() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(INPUT_STREAM_3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(OUTPUT_STREAM_3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(STANDARD_OUTPUT_DATA));

        System.out.println(CLUSTER.getAllTopicsInCluster());
        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(changelog1, changelog2, changelog3)), is(true));
    }

    @Test
    public void shouldAllowPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        topology1Builder.stream(Pattern.compile(INPUT_STREAM_1)).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(Pattern.compile(INPUT_STREAM_2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(Pattern.compile(INPUT_STREAM_3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(STANDARD_OUTPUT_DATA));
    }

    @Test
    public void shouldAllowMixedCollectionAndPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(Pattern.compile(INPUT_STREAM_2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(Pattern.compile(INPUT_STREAM_3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(OUTPUT_STREAM_3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(STANDARD_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(STANDARD_OUTPUT_DATA));
    }

    private static void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            records,
            producerConfig,
            CLUSTER.time
        );
    }

    private List<NamedTopology> buildNamedTopologies(final NamedTopologyStreamsBuilder... builders) {
        final List<NamedTopology> topologies = new ArrayList<>();
        for (final NamedTopologyStreamsBuilder builder : builders) {
            topologies.add(builder.buildNamedTopology(props));
        }
        return topologies;
    }
}
