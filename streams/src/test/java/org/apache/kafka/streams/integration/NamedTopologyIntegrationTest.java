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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class NamedTopologyIntegrationTest {

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TestName testName = new TestName();
    private String appId;

    private String inputStream1;
    private String inputStream2;
    private String inputStream3;
    private String outputStream1;
    private String outputStream2;
    private String outputStream3;
    private String storeChangelog1;
    private String storeChangelog2;
    private String storeChangelog3;

    final List<KeyValue<String, Long>> standardInputData = asList(KeyValue.pair("A", 100L), KeyValue.pair("B", 200L), KeyValue.pair("A", 300L), KeyValue.pair("C", 400L));
    final List<KeyValue<String, Long>> standardOutputData = asList(KeyValue.pair("B", 1L), KeyValue.pair("A", 2L), KeyValue.pair("C", 1L)); // output of basic count topology with caching

    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, LongSerializer.class);
    final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);

    final NamedTopologyStreamsBuilder builder1 = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder builder2 = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder builder3 = new NamedTopologyStreamsBuilder("topology-3");

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
    public void setup() throws InterruptedException {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        inputStream1 = appId + "-input-stream-1";
        inputStream2 = appId + "-input-stream-2";
        inputStream3 = appId + "-input-stream-3";
        outputStream1 = appId + "-output-stream-1";
        outputStream2 = appId + "-output-stream-2";
        outputStream3 = appId + "-output-stream-3";
        storeChangelog1 = appId + "-topology-1-store-changelog";
        storeChangelog2 = appId + "-topology-2-store-changelog";
        storeChangelog3 = appId + "-topology-3-store-changelog";
        props = configProps();
        CLUSTER.createTopic(inputStream1, 2, 1);
        CLUSTER.createTopic(inputStream2, 2, 1);
        CLUSTER.createTopic(inputStream3, 2, 1);
        CLUSTER.createTopic(outputStream1, 2, 1);
        CLUSTER.createTopic(outputStream2, 2, 1);
        CLUSTER.createTopic(outputStream3, 2, 1);
    }

    @After
    public void shutdown() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        CLUSTER.deleteTopics(inputStream1, inputStream2, inputStream3, outputStream1, outputStream2, outputStream3);
    }

    @Test
    public void shouldProcessSingleNamedTopologyAndPrefixInternalTopics() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        builder1.stream(inputStream1)
            .selectKey((k, v) -> k)
            .groupByKey()
            .count(Materialized.as(Stores.persistentKeyValueStore("store")))
            .toStream().to(outputStream1);
        streams = new KafkaStreamsNamedTopologyWrapper(builder1.buildNamedTopology(props), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
        final List<KeyValue<String, Long>> results = waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3);
        assertThat(results, equalTo(standardOutputData));

        final Set<String> allTopics = CLUSTER.getAllTopicsInCluster();
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-changelog"), is(true));
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-repartition"), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithPersistentStateStores() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        builder1.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream1);
        builder2.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream2);
        builder3.stream(inputStream3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1, builder2, builder3), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(storeChangelog1, storeChangelog2, storeChangelog3)), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryStateStores() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        builder1.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        builder2.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);
        builder3.stream(inputStream3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1, builder2, builder3), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(storeChangelog1, storeChangelog2, storeChangelog3)), is(true));
    }

    @Test
    public void shouldAllowPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        builder1.stream(Pattern.compile(inputStream1)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream1);
        builder2.stream(Pattern.compile(inputStream2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream2);
        builder3.stream(Pattern.compile(inputStream3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1, builder2, builder3), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));
    }

    @Test
    public void shouldAllowMixedCollectionAndPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        builder1.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream1);
        builder2.stream(Pattern.compile(inputStream2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream2);
        builder3.stream(Pattern.compile(inputStream3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1, builder2, builder3), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));
    }

    private void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
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
