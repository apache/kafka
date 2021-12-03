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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.namedtopology.AddNamedTopologyResult;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.streams.processor.internals.namedtopology.RemoveNamedTopologyResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class NamedTopologyIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String TOPOLOGY_1 = "topology-1";
    private static final String TOPOLOGY_2 = "topology-2";
    private static final String TOPOLOGY_3 = "topology-3";

    // TODO KAFKA-12648:
    //  1) full test coverage for add/removeNamedTopology, covering:
    //      - the "last topology removed" case
    //      - test using multiple clients, with standbys

    // "standard" input topics which are pre-filled with the STANDARD_INPUT_DATA
    private final static String INPUT_STREAM_1 = "input-stream-1";
    private final static String INPUT_STREAM_2 = "input-stream-2";
    private final static String INPUT_STREAM_3 = "input-stream-3";

    private final static String OUTPUT_STREAM_1 = "output-stream-1";
    private final static String OUTPUT_STREAM_2 = "output-stream-2";
    private final static String OUTPUT_STREAM_3 = "output-stream-3";

    private final static String SUM_OUTPUT = "sum";
    private final static String COUNT_OUTPUT = "count";

    // "delayed" input topics which are empty at start to allow control over when input data appears
    private final static String DELAYED_INPUT_STREAM_1 = "delayed-input-stream-1";
    private final static String DELAYED_INPUT_STREAM_2 = "delayed-input-stream-2";
    private final static String DELAYED_INPUT_STREAM_3 = "delayed-input-stream-3";
    private final static String DELAYED_INPUT_STREAM_4 = "delayed-input-stream-4";


    private final static Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> IN_MEMORY_STORE = Materialized.as(Stores.inMemoryKeyValueStore("store"));
    private final static Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> ROCKSDB_STORE = Materialized.as(Stores.persistentKeyValueStore("store"));

    private static Properties producerConfig;
    private static Properties consumerConfig;

    @BeforeClass
    public static void initializeClusterAndStandardTopics() throws Exception {
        CLUSTER.start();

        CLUSTER.createTopic(INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_3, 2, 1);

        CLUSTER.createTopic(DELAYED_INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_3, 2, 1);

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

    private final static List<KeyValue<String, Long>> STANDARD_INPUT_DATA =
        asList(pair("A", 100L), pair("B", 200L), pair("A", 300L), pair("C", 400L), pair("C", -50L));
    private final static List<KeyValue<String, Long>> COUNT_OUTPUT_DATA =
        asList(pair("B", 1L), pair("A", 2L), pair("C", 2L)); // output of count operation with caching
    private final static List<KeyValue<String, Long>> SUM_OUTPUT_DATA =
        asList(pair("B", 200L), pair("A", 400L), pair("C", 350L)); // output of summation with caching

    private final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    private Properties props;
    private Properties props2;

    private KafkaStreamsNamedTopologyWrapper streams;
    private KafkaStreamsNamedTopologyWrapper streams2;

    // builders for the 1st Streams instance (default)
    private NamedTopologyBuilder topology1Builder;
    private NamedTopologyBuilder topology1BuilderDup;
    private NamedTopologyBuilder topology2Builder;
    private NamedTopologyBuilder topology3Builder;

    // builders for the 2nd Streams instance
    private NamedTopologyBuilder topology1Builder2;
    private NamedTopologyBuilder topology2Builder2;
    private NamedTopologyBuilder topology3Builder2;

    private Properties configProps(final String appId) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);
        return streamsConfiguration;
    }

    @Before
    public void setup() throws Exception {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        changelog1 = appId + "-" + TOPOLOGY_1 + "-store-changelog";
        changelog2 = appId + "-" + TOPOLOGY_2 + "-store-changelog";
        changelog3 = appId + "-" + TOPOLOGY_3 + "-store-changelog";
        props = configProps(appId);

        streams = new KafkaStreamsNamedTopologyWrapper(props, clientSupplier);

        topology1Builder = streams.newNamedTopologyBuilder(TOPOLOGY_1);
        topology1BuilderDup = streams.newNamedTopologyBuilder(TOPOLOGY_1);
        topology2Builder = streams.newNamedTopologyBuilder(TOPOLOGY_2);
        topology3Builder = streams.newNamedTopologyBuilder(TOPOLOGY_3);

        // TODO KAFKA-12648: refactor to avoid deleting & (re)creating outputs topics for each test
        CLUSTER.createTopic(OUTPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_3, 2, 1);
    }

    private void setupSecondKafkaStreams() {
        props2 = configProps(appId);
        streams2 = new KafkaStreamsNamedTopologyWrapper(props2, clientSupplier);
        topology1Builder2 = streams2.newNamedTopologyBuilder(TOPOLOGY_1);
        topology2Builder2 = streams2.newNamedTopologyBuilder(TOPOLOGY_2);
        topology3Builder2 = streams2.newNamedTopologyBuilder(TOPOLOGY_3);
    }

    @After
    public void shutdown() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }

        CLUSTER.getAllTopicsInCluster().stream().filter(t -> t.contains("-changelog")).forEach(t -> {
            try {
                CLUSTER.deleteTopicsAndWait(t);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        });

        CLUSTER.deleteTopicsAndWait(OUTPUT_STREAM_1, OUTPUT_STREAM_2, OUTPUT_STREAM_3);
        CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
    }

    @Test
    public void shouldPrefixAllInternalTopicNamesWithNamedTopology() throws Exception {
        final String countTopologyName = "count-topology";
        final String fkjTopologyName = "FKJ-topology";

        final NamedTopologyBuilder countBuilder = streams.newNamedTopologyBuilder(countTopologyName);
        countBuilder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count();

        final NamedTopologyBuilder fkjBuilder = streams.newNamedTopologyBuilder(fkjTopologyName);

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

        streams.start(asList(fkjBuilder.build(), countBuilder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(60));

        final String countTopicPrefix = appId + "-" + countTopologyName;
        final String fkjTopicPrefix = appId + "-" + fkjTopologyName;
        final  Set<String> internalTopics = CLUSTER
            .getAllTopicsInCluster().stream()
            .filter(t -> t.contains(appId))
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
            .count(ROCKSDB_STORE)
            .toStream().to(OUTPUT_STREAM_1);
        streams.start(topology1Builder.build());
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));
        final List<KeyValue<String, Long>> results = waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3);
        assertThat(results, equalTo(COUNT_OUTPUT_DATA));

        final Set<String> allTopics = CLUSTER.getAllTopicsInCluster();
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-changelog"), is(true));
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-repartition"), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryAndPersistentStateStores() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(INPUT_STREAM_3).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build(), topology3Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(changelog1, changelog2, changelog3)), is(true));
    }

    @Test
    public void shouldAddNamedTopologyToUnstartedApplicationWithEmptyInitialTopology() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        streams.addNamedTopology(topology1Builder.build());
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
    }
    
    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithEmptyInitialTopology() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        streams.start();
        streams.addNamedTopology(topology1Builder.build()).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithSingleInitialNamedTopology() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        streams.start(topology1Builder.build());
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));
        streams.addNamedTopology(topology2Builder.build()).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithMultipleInitialNamedTopologies() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(INPUT_STREAM_3).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        streams.addNamedTopology(topology3Builder.build()).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    @Ignore
    public void shouldAddNamedTopologyToRunningApplicationWithMultipleNodes() throws Exception {
        setupSecondKafkaStreams();
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology1Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        topology2Builder2.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);

        streams.start(topology1Builder.build());
        streams2.start(topology1Builder2.build());
        waitForApplicationState(asList(streams, streams2), State.RUNNING, Duration.ofSeconds(30));

        final AddNamedTopologyResult result = streams.addNamedTopology(topology2Builder.build());
        final AddNamedTopologyResult result2 = streams2.addNamedTopology(topology2Builder2.build());
        result.all().get();
        result2.all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));

        // TODO KAFKA-12648: need to make sure that both instances actually did some of this processing of topology-2,
        //  ie that both joined the group after the new topology was added and then successfully processed records from it
        //  Also: test where we wait for a rebalance between streams.addNamedTopology and streams2.addNamedTopology,
        //  and vice versa, to make sure we hit case where not all new tasks are initially assigned, and when not all yet known
    }

    @Test
    public void shouldRemoveNamedTopologyToRunningApplicationWithMultipleNodesAndResetsOffsets() throws Exception {
        setupSecondKafkaStreams();
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology1Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

        streams.start(topology1Builder.build());
        streams2.start(topology1Builder2.build());
        waitForApplicationState(asList(streams, streams2), State.RUNNING, Duration.ofSeconds(30));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));

        final RemoveNamedTopologyResult result = streams.removeNamedTopology(TOPOLOGY_1, true);
        streams2.removeNamedTopology(TOPOLOGY_1, true).all().get();
        result.all().get();

        assertThat(streams.getTopologyByName(TOPOLOGY_1), equalTo(Optional.empty()));
        assertThat(streams2.getTopologyByName(TOPOLOGY_1), equalTo(Optional.empty()));

        streams.cleanUpNamedTopology(TOPOLOGY_1);
        streams2.cleanUpNamedTopology(TOPOLOGY_1);

        CLUSTER.getAllTopicsInCluster().stream().filter(t -> t.contains("-changelog")).forEach(t -> {
            try {
                CLUSTER.deleteTopicAndWait(t);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        });

        topology2Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        topology2Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);

        final AddNamedTopologyResult result1 = streams.addNamedTopology(topology2Builder.build());
        streams2.addNamedTopology(topology2Builder2.build()).all().get();
        result1.all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldRemoveOneNamedTopologyWhileAnotherContinuesProcessing() throws Exception {
        topology1Builder.stream(DELAYED_INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(DELAYED_INPUT_STREAM_2).map((k, v) -> {
            throw new IllegalStateException("Should not process any records for removed topology-2");
        });
        streams.start(asList(topology1Builder.build(), topology2Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        streams.removeNamedTopology("topology-2").all().get();

        produceToInputTopics(DELAYED_INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(DELAYED_INPUT_STREAM_2, STANDARD_INPUT_DATA);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldRemoveAndReplaceTopologicallyIncompatibleNamedTopology() throws Exception {
        CLUSTER.createTopics(SUM_OUTPUT, COUNT_OUTPUT);
        // Build up named topology with two stateful subtopologies
        final KStream<String, Long> inputStream1 = topology1Builder.stream(INPUT_STREAM_1);
        inputStream1.groupByKey().count().toStream().to(COUNT_OUTPUT);
        inputStream1.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
        streams.start(singletonList(topology1Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
        streams.removeNamedTopology(TOPOLOGY_1).all().get();
        streams.cleanUpNamedTopology(TOPOLOGY_1);

        // Prepare a new named topology with the same name but an incompatible topology (stateful subtopologies swap order)
        final NamedTopologyBuilder topology1Builder2 = streams.newNamedTopologyBuilder(TOPOLOGY_1);
        final KStream<String, Long> inputStream2 = topology1Builder2.stream(DELAYED_INPUT_STREAM_4);
        inputStream2.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
        inputStream2.groupByKey().count().toStream().to(COUNT_OUTPUT);

        produceToInputTopics(DELAYED_INPUT_STREAM_4, STANDARD_INPUT_DATA);
        streams.addNamedTopology(topology1Builder2.build()).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
        CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
    }
    
    @Test
    public void shouldAllowPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        topology1Builder.stream(Pattern.compile(INPUT_STREAM_1)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(Pattern.compile(INPUT_STREAM_2)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(Pattern.compile(INPUT_STREAM_3)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build(), topology3Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAllowMixedCollectionAndPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(Pattern.compile(INPUT_STREAM_2)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(Pattern.compile(INPUT_STREAM_3)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build(), topology3Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(30));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAddToEmptyInitialTopologyRemoveResetOffsetsThenAddSameNamedTopology() throws Exception {
        CLUSTER.createTopics(SUM_OUTPUT, COUNT_OUTPUT);
        // Build up named topology with two stateful subtopologies
        final KStream<String, Long> inputStream1 = topology1Builder.stream(INPUT_STREAM_1);
        inputStream1.groupByKey().count().toStream().to(COUNT_OUTPUT);
        inputStream1.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
        streams.start();
        final NamedTopology namedTopology = topology1Builder.build();
        streams.addNamedTopology(namedTopology).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
        streams.removeNamedTopology("topology-1", true).all().get();
        streams.cleanUpNamedTopology("topology-1");

        CLUSTER.getAllTopicsInCluster().stream().filter(t -> t.contains("changelog")).forEach(t -> {
            try {
                CLUSTER.deleteTopicAndWait(t);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        });

        final KStream<String, Long> inputStream = topology1BuilderDup.stream(INPUT_STREAM_1);
        inputStream.groupByKey().count().toStream().to(COUNT_OUTPUT);
        inputStream.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);

        final NamedTopology namedTopologyDup = topology1BuilderDup.build();
        streams.addNamedTopology(namedTopologyDup).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));

        CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
    }

    @Test
    @Ignore
    public void shouldAddToEmptyInitialTopologyRemoveResetOffsetsThenAddSameNamedTopologyWithRepartitioning() throws Exception {
        CLUSTER.createTopics(SUM_OUTPUT, COUNT_OUTPUT);
        // Build up named topology with two stateful subtopologies
        final KStream<String, Long> inputStream1 = topology1Builder.stream(INPUT_STREAM_1);
        inputStream1.map(KeyValue::new).groupByKey().count().toStream().to(COUNT_OUTPUT);
        inputStream1.map(KeyValue::new).groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
        streams.start();
        final NamedTopology namedTopology = topology1Builder.build();
        streams.addNamedTopology(namedTopology).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
        streams.removeNamedTopology(TOPOLOGY_1, true).all().get();
        streams.cleanUpNamedTopology(TOPOLOGY_1);

        CLUSTER.getAllTopicsInCluster().stream().filter(t -> t.contains("-changelog") || t.contains("-repartition")).forEach(t -> {
            try {
                CLUSTER.deleteTopicsAndWait(t);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        });

        final KStream<String, Long> inputStream = topology1BuilderDup.stream(INPUT_STREAM_1);
        inputStream.map(KeyValue::new).groupByKey().count().toStream().to(COUNT_OUTPUT);
        inputStream.map(KeyValue::new).groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);

        final NamedTopology namedTopologyDup = topology1BuilderDup.build();
        streams.addNamedTopology(namedTopologyDup).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));

        CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
    }


    private static void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            records,
            producerConfig,
            CLUSTER.time
        );
    }
}
