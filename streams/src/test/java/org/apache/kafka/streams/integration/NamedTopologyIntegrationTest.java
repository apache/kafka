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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
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
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.processor.internals.namedtopology.RemoveNamedTopologyResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.KeyQueryMetadata.NOT_AVAILABLE;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.processor.internals.ClientUtils.extractThreadId;
import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

@Category(IntegrationTest.class)
public class NamedTopologyIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(45);
    
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String TOPOLOGY_1 = "topology-1";
    private static final String TOPOLOGY_2 = "topology-2";
    private static final String TOPOLOGY_3 = "topology-3";

    // "standard" input topics which are pre-filled with the STANDARD_INPUT_DATA
    private static final String INPUT_STREAM_1 = "input-stream-1";
    private static final String INPUT_STREAM_2 = "input-stream-2";
    private static final String INPUT_STREAM_3 = "input-stream-3";

    private static final String OUTPUT_STREAM_1 = "output-stream-1";
    private static final String OUTPUT_STREAM_2 = "output-stream-2";
    private static final String OUTPUT_STREAM_3 = "output-stream-3";

    private static final String SUM_OUTPUT = "sum";
    private static final String COUNT_OUTPUT = "count";

    // "delayed" input topics which are empty at start to allow control over when input data appears
    private static final String DELAYED_INPUT_STREAM_1 = "delayed-input-stream-1";
    private static final String DELAYED_INPUT_STREAM_2 = "delayed-input-stream-2";

    // topic that is not initially created during the test setup
    private static final String NEW_STREAM = "new-stream";

    // existing topic that is pre-filled but cleared between tests
    private static final String EXISTING_STREAM = "existing-stream";

    // topic created with just one partition
    private static final String SINGLE_PARTITION_INPUT_STREAM = "single-partition-input-stream";
    private static final String SINGLE_PARTITION_OUTPUT_STREAM = "single-partition-output-stream";

    private static final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> IN_MEMORY_STORE = Materialized.as(Stores.inMemoryKeyValueStore("store"));
    private static final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> ROCKSDB_STORE = Materialized.as(Stores.persistentKeyValueStore("store"));

    private static Properties producerConfig;
    private static Properties consumerConfig;

    @BeforeClass
    public static void initializeClusterAndStandardTopics() throws Exception {
        CLUSTER.start();

        CLUSTER.createTopic(INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_3, 2, 1);

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

    private static final List<KeyValue<String, Long>> STANDARD_INPUT_DATA =
        asList(pair("A", 100L), pair("B", 200L), pair("A", 300L), pair("C", 400L), pair("C", -50L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA =
        asList(pair("B", 1L), pair("A", 2L), pair("C", 2L)); // output of count operation with caching
    private static final List<KeyValue<String, Long>> SUM_OUTPUT_DATA =
        asList(pair("B", 200L), pair("A", 400L), pair("C", 350L)); // output of summation with caching
    private static final String TOPIC_PREFIX = "unique_topic_prefix";

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

    private Properties configProps(final String appId, final String host) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":2020");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, TOPIC_PREFIX);
        return streamsConfiguration;
    }

    @Before
    public void setup() throws Exception {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        changelog1 = TOPIC_PREFIX + "-" + TOPOLOGY_1 + "-store-changelog";
        changelog2 = TOPIC_PREFIX + "-" + TOPOLOGY_2 + "-store-changelog";
        changelog3 = TOPIC_PREFIX + "-" + TOPOLOGY_3 + "-store-changelog";
        props = configProps(appId, "host1");
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
        props2 = configProps(appId, "host2");
        streams2 = new KafkaStreamsNamedTopologyWrapper(props2, clientSupplier);
        topology1Builder2 = streams2.newNamedTopologyBuilder(TOPOLOGY_1);
        topology2Builder2 = streams2.newNamedTopologyBuilder(TOPOLOGY_2);
    }

    @After
    public void shutdown() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }

        CLUSTER.getAllTopicsInCluster().stream().filter(t -> t.contains("-changelog") || t.contains("-repartition")).forEach(t -> {
            try {
                assertThat("topic was not decorated", t.contains(TOPIC_PREFIX));
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
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        final String countTopicPrefix = TOPIC_PREFIX + "-" + countTopologyName;
        final String fkjTopicPrefix = TOPIC_PREFIX + "-" + fkjTopologyName;
        final  Set<String> internalTopics = CLUSTER
            .getAllTopicsInCluster().stream()
            .filter(t -> t.contains(TOPIC_PREFIX))
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
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);
        final List<KeyValue<String, Long>> results = waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3);
        assertThat(results, equalTo(COUNT_OUTPUT_DATA));

        final Set<String> allTopics = CLUSTER.getAllTopicsInCluster();
        assertThat(allTopics.contains(TOPIC_PREFIX + "-" + "topology-1" + "-store-changelog"), is(true));
        assertThat(allTopics.contains(TOPIC_PREFIX + "-" + "topology-1" + "-store-repartition"), is(true));
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryAndPersistentStateStores() throws Exception {
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(INPUT_STREAM_3).groupBy((k, v) -> k).count(ROCKSDB_STORE).toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build(), topology3Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(changelog1, changelog2, changelog3)), is(true));
    }

    @Test
    public void shouldAddAndRemoveNamedTopologiesBeforeStartingAndRouteQueriesToCorrectTopology() throws Exception {
        try {
            // for this test we have one of the topologies read from an input topic with just one partition so
            // that there's only one instance of that topology's store and thus should always have exactly one
            // StreamsMetadata returned by any of the methods that look up all hosts with a specific store and topology
            CLUSTER.createTopic(SINGLE_PARTITION_INPUT_STREAM, 1, 1);
            CLUSTER.createTopic(SINGLE_PARTITION_OUTPUT_STREAM, 1, 1);
            produceToInputTopics(SINGLE_PARTITION_INPUT_STREAM, STANDARD_INPUT_DATA);

            final String topology1Store = "store-" + TOPOLOGY_1;
            final String topology2Store = "store-" + TOPOLOGY_2;

            topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(Materialized.as(topology1Store)).toStream().to(OUTPUT_STREAM_1);
            topology2Builder.stream(SINGLE_PARTITION_INPUT_STREAM).groupByKey().count(Materialized.as(topology2Store)).toStream().to(SINGLE_PARTITION_OUTPUT_STREAM);
            streams.addNamedTopology(topology1Builder.build());
            streams.removeNamedTopology(TOPOLOGY_1);
            assertThat(streams.getTopologyByName(TOPOLOGY_1), is(Optional.empty()));
            streams.addNamedTopology(topology1Builder.build());
            streams.addNamedTopology(topology2Builder.build());
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SINGLE_PARTITION_OUTPUT_STREAM, 3), equalTo(COUNT_OUTPUT_DATA));

            final ReadOnlyKeyValueStore<String, Long> store =
                streams.store(NamedTopologyStoreQueryParameters.fromNamedTopologyAndStoreNameAndType(
                    TOPOLOGY_1,
                    topology1Store,
                    QueryableStoreTypes.keyValueStore())
                );
            assertThat(store.get("A"), equalTo(2L));

            final Collection<StreamsMetadata> streamsMetadata = streams.streamsMetadataForStore(topology1Store, TOPOLOGY_1);
            final Collection<StreamsMetadata> streamsMetadata2 = streams.streamsMetadataForStore(topology2Store, TOPOLOGY_2);
            assertThat(streamsMetadata.size(), equalTo(1));
            assertThat(streamsMetadata2.size(), equalTo(1));

            final KeyQueryMetadata keyMetadata = streams.queryMetadataForKey(topology1Store, "A", new StringSerializer(), TOPOLOGY_1);
            final KeyQueryMetadata keyMetadata2 = streams.queryMetadataForKey(topology2Store, "A", new StringSerializer(), TOPOLOGY_2);

            assertThat(keyMetadata, not(NOT_AVAILABLE));
            assertThat(keyMetadata, equalTo(keyMetadata2));

            final Map<String, Map<Integer, LagInfo>> partitionLags1 = streams.allLocalStorePartitionLagsForTopology(TOPOLOGY_1);
            final Map<String, Map<Integer, LagInfo>> partitionLags2 = streams.allLocalStorePartitionLagsForTopology(TOPOLOGY_2);

            assertThat(partitionLags1.keySet(), equalTo(singleton(topology1Store)));
            assertThat(partitionLags1.get(topology1Store).keySet(), equalTo(mkSet(0, 1)));
            assertThat(partitionLags2.keySet(), equalTo(singleton(topology2Store)));
            assertThat(partitionLags2.get(topology2Store).keySet(), equalTo(singleton(0))); // only one copy of the store in topology-2

            // Start up a second node with both topologies
            setupSecondKafkaStreams();

            topology1Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(Materialized.as(topology1Store)).toStream().to(OUTPUT_STREAM_1);
            topology2Builder2.stream(SINGLE_PARTITION_INPUT_STREAM).groupByKey().count(Materialized.as(topology2Store)).toStream().to(SINGLE_PARTITION_OUTPUT_STREAM);

            streams2.start(asList(topology1Builder2.build(), topology2Builder2.build()));
            waitForApplicationState(asList(streams, streams2), State.RUNNING, STARTUP_TIMEOUT);

            verifyMetadataForTopology(
                TOPOLOGY_1,
                streams.streamsMetadataForStore(topology1Store, TOPOLOGY_1),
                streams2.streamsMetadataForStore(topology1Store, TOPOLOGY_1));
            verifyMetadataForTopology(
                TOPOLOGY_2,
                streams.streamsMetadataForStore(topology2Store, TOPOLOGY_2),
                streams2.streamsMetadataForStore(topology2Store, TOPOLOGY_2));

            assertThat(streams.allStreamsClientsMetadataForTopology(TOPOLOGY_1).size(), equalTo(2));
            assertThat(streams2.allStreamsClientsMetadataForTopology(TOPOLOGY_1).size(), equalTo(2));
            verifyMetadataForTopology(
                TOPOLOGY_1,
                streams.allStreamsClientsMetadataForTopology(TOPOLOGY_1),
                streams2.allStreamsClientsMetadataForTopology(TOPOLOGY_1));
            assertThat(streams.allStreamsClientsMetadataForTopology(TOPOLOGY_2).size(), equalTo(2));
            assertThat(streams2.allStreamsClientsMetadataForTopology(TOPOLOGY_2).size(), equalTo(2));
            verifyMetadataForTopology(
                TOPOLOGY_2,
                streams.allStreamsClientsMetadataForTopology(TOPOLOGY_2),
                streams2.allStreamsClientsMetadataForTopology(TOPOLOGY_2));

        } finally {
            CLUSTER.deleteTopics(SINGLE_PARTITION_INPUT_STREAM, SINGLE_PARTITION_OUTPUT_STREAM);
        }
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
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);
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
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        streams.addNamedTopology(topology3Builder.build()).all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithMultipleNodes() throws Exception {
        setupSecondKafkaStreams();
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology1Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

        topology2Builder.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
        topology2Builder2.stream(INPUT_STREAM_2).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);

        streams.start(topology1Builder.build());
        streams2.start(topology1Builder2.build());
        waitForApplicationState(asList(streams, streams2), State.RUNNING, STARTUP_TIMEOUT);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));

        final AddNamedTopologyResult result = streams.addNamedTopology(topology2Builder.build());
        final AddNamedTopologyResult result2 = streams2.addNamedTopology(topology2Builder2.build());
        result.all().get();
        result2.all().get();

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAllowRemovingAndAddingNamedTopologyToRunningApplicationWithMultipleNodesAndResetsOffsets() throws Exception {
        setupSecondKafkaStreams();
        topology1Builder.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology1Builder2.stream(INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

        streams.start(topology1Builder.build());
        streams2.start(topology1Builder2.build());
        waitForApplicationState(asList(streams, streams2), State.RUNNING, STARTUP_TIMEOUT);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));

        final RemoveNamedTopologyResult result = streams.removeNamedTopology(TOPOLOGY_1, true);
        streams2.removeNamedTopology(TOPOLOGY_1, true).all().get();
        result.all().get();

        assertThat(streams.getTopologyByName(TOPOLOGY_1), equalTo(Optional.empty()));
        assertThat(streams2.getTopologyByName(TOPOLOGY_1), equalTo(Optional.empty()));

        assertThat(streams.getAllTopologies().isEmpty(), is(true));
        assertThat(streams2.getAllTopologies().isEmpty(), is(true));

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

        final NamedTopology topology2Client1 = topology2Builder.build();
        final NamedTopology topology2Client2 = topology2Builder2.build();

        final AddNamedTopologyResult result1 = streams.addNamedTopology(topology2Client1);
        streams2.addNamedTopology(topology2Client2).all().get();
        result1.all().get();

        assertThat(streams.getAllTopologies(), equalTo(singleton(topology2Client1)));
        assertThat(streams2.getAllTopologies(), equalTo(singleton(topology2Client2)));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldRemoveOneNamedTopologyWhileAnotherContinuesProcessing() throws Exception {
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_2, 2, 1);

        try {
            topology1Builder.stream(DELAYED_INPUT_STREAM_1).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
            topology2Builder.stream(DELAYED_INPUT_STREAM_2).map((k, v) -> {
                throw new IllegalStateException("Should not process any records for removed topology-2");
            });
            streams.start(asList(topology1Builder.build(), topology2Builder.build()));
            waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

            streams.removeNamedTopology("topology-2").all().get();

            produceToInputTopics(DELAYED_INPUT_STREAM_1, STANDARD_INPUT_DATA);
            produceToInputTopics(DELAYED_INPUT_STREAM_2, STANDARD_INPUT_DATA);

            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        } finally {
            CLUSTER.deleteTopics(DELAYED_INPUT_STREAM_1, DELAYED_INPUT_STREAM_2);
        }
    }

    @Test
    public void shouldRemoveAndReplaceTopologicallyIncompatibleNamedTopology() throws Exception {
        CLUSTER.createTopics(SUM_OUTPUT, COUNT_OUTPUT);
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_1, 2, 1);

        try {
            // Build up named topology with two stateful subtopologies
            final KStream<String, Long> inputStream1 = topology1Builder.stream(INPUT_STREAM_1);
            inputStream1.groupByKey().count().toStream().to(COUNT_OUTPUT);
            inputStream1.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
            streams.start(singletonList(topology1Builder.build()));
            waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
            streams.removeNamedTopology(TOPOLOGY_1).all().get();
            streams.cleanUpNamedTopology(TOPOLOGY_1);

            // Prepare a new named topology with the same name but an incompatible topology (stateful subtopologies swap order)
            final NamedTopologyBuilder topology1Builder2 = streams.newNamedTopologyBuilder(TOPOLOGY_1);
            final KStream<String, Long> inputStream2 = topology1Builder2.stream(DELAYED_INPUT_STREAM_1);
            inputStream2.groupByKey().reduce(Long::sum).toStream().to(SUM_OUTPUT);
            inputStream2.groupByKey().count().toStream().to(COUNT_OUTPUT);

            produceToInputTopics(DELAYED_INPUT_STREAM_1, STANDARD_INPUT_DATA);
            streams.addNamedTopology(topology1Builder2.build()).all().get();

            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, COUNT_OUTPUT, 3), equalTo(COUNT_OUTPUT_DATA));
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, SUM_OUTPUT, 3), equalTo(SUM_OUTPUT_DATA));
        } finally {
            CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
            CLUSTER.deleteTopics(DELAYED_INPUT_STREAM_1);
        }
    }
    
    @Test
    public void shouldAllowPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        topology1Builder.stream(Pattern.compile(INPUT_STREAM_1)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_1);
        topology2Builder.stream(Pattern.compile(INPUT_STREAM_2)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_2);
        topology3Builder.stream(Pattern.compile(INPUT_STREAM_3)).groupBy((k, v) -> k).count().toStream().to(OUTPUT_STREAM_3);
        streams.start(asList(topology1Builder.build(), topology2Builder.build(), topology3Builder.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

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
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_3, 3), equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldAddToEmptyInitialTopologyRemoveResetOffsetsThenAddSameNamedTopology() throws Exception {
        CLUSTER.createTopics(SUM_OUTPUT, COUNT_OUTPUT);
        try {
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
        } finally {
            CLUSTER.deleteTopicsAndWait(SUM_OUTPUT, COUNT_OUTPUT);
        }
    }

    @Test
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

    @Test
    public void shouldContinueProcessingOtherTopologiesWhenNewTopologyHasMissingInputTopics() throws Exception {
        try {
            CLUSTER.createTopic(EXISTING_STREAM, 2, 1);
            produceToInputTopics(EXISTING_STREAM, STANDARD_INPUT_DATA);
            setupSecondKafkaStreams();
            topology1Builder.stream(EXISTING_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
            topology1Builder2.stream(EXISTING_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

            final TrackingExceptionHandler handler = new TrackingExceptionHandler();
            streams.setUncaughtExceptionHandler(handler);
            streams2.setUncaughtExceptionHandler(handler);

            streams.start(topology1Builder.build());
            streams2.start(topology1Builder2.build());
            waitForApplicationState(asList(streams, streams2), State.RUNNING, STARTUP_TIMEOUT);
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3), equalTo(COUNT_OUTPUT_DATA));

            topology2Builder.stream(NEW_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);
            topology2Builder2.stream(NEW_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_2);

            assertThat(handler.nextError(TOPOLOGY_2), nullValue());

            streams.addNamedTopology(topology2Builder.build());
            streams2.addNamedTopology(topology2Builder2.build());

            // verify that the missing source topics were noticed and the handler invoked
            retryOnExceptionWithTimeout(() -> {
                final Throwable error = handler.nextError(TOPOLOGY_2);
                assertThat(error, notNullValue());
                assertThat(error.getCause().getClass(), is(MissingSourceTopicException.class));
            });

            // make sure the original topology can continue processing while waiting on the new source topics
            produceToInputTopics(EXISTING_STREAM, singletonList(pair("A", 30L)));
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 1), equalTo(singletonList(pair("A", 3L))));

            CLUSTER.createTopic(NEW_STREAM, 2, 1);
            produceToInputTopics(NEW_STREAM, STANDARD_INPUT_DATA);
            assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3), equalTo(COUNT_OUTPUT_DATA));

            // Make sure the threads were not actually killed and replaced
            assertThat(streams.metadataForLocalThreads().size(), equalTo(2));
            assertThat(streams2.metadataForLocalThreads().size(), equalTo(2));

            final Set<String> localThreadsNames = streams.metadataForLocalThreads().stream()
                .map(t -> extractThreadId(t.threadName()))
                .collect(Collectors.toSet());
            final Set<String> localThreadsNames2 = streams2.metadataForLocalThreads().stream()
                .map(t -> extractThreadId(t.threadName()))
                .collect(Collectors.toSet());

            assertThat(localThreadsNames.contains("StreamThread-1"), is(true));
            assertThat(localThreadsNames.contains("StreamThread-2"), is(true));
            assertThat(localThreadsNames2.contains("StreamThread-1"), is(true));
            assertThat(localThreadsNames2.contains("StreamThread-2"), is(true));
        } finally {
            CLUSTER.deleteTopicsAndWait(EXISTING_STREAM, NEW_STREAM);
        }
    }

    @Test
    public void shouldWaitForMissingInputTopicsToBeCreated() throws Exception {
        setupSecondKafkaStreams();
        topology1Builder.stream(NEW_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);
        topology1Builder2.stream(NEW_STREAM).groupBy((k, v) -> k).count(IN_MEMORY_STORE).toStream().to(OUTPUT_STREAM_1);

        final TrackingExceptionHandler handler = new  TrackingExceptionHandler();
        streams.setUncaughtExceptionHandler(handler);
        streams2.setUncaughtExceptionHandler(handler);

        streams.start(topology1Builder.build());
        streams2.start(topology1Builder2.build());
        waitForApplicationState(asList(streams, streams2), State.RUNNING, STARTUP_TIMEOUT);

        retryOnExceptionWithTimeout(() -> {
            final Throwable error = handler.nextError(TOPOLOGY_1);
            assertThat(error, notNullValue());
            assertThat(error.getCause().getClass(), is(MissingSourceTopicException.class));
        });

        try {
            CLUSTER.createTopic(NEW_STREAM, 2, 1);
            produceToInputTopics(NEW_STREAM, STANDARD_INPUT_DATA);

            final List<KeyValue<String, Integer>> output =
                waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3);
            output.retainAll(COUNT_OUTPUT_DATA);

            assertThat(output, equalTo(COUNT_OUTPUT_DATA));

            // Make sure the threads were not actually killed and replaced
            assertThat(streams.metadataForLocalThreads().size(), equalTo(2));
            assertThat(streams2.metadataForLocalThreads().size(), equalTo(2));

            final Set<String> localThreadsNames = streams.metadataForLocalThreads().stream()
                .map(t -> extractThreadId(t.threadName()))
                .collect(Collectors.toSet());
            final Set<String> localThreadsNames2 = streams2.metadataForLocalThreads().stream()
                .map(t -> extractThreadId(t.threadName()))
                .collect(Collectors.toSet());

            assertThat(localThreadsNames.contains("StreamThread-1"), is(true));
            assertThat(localThreadsNames.contains("StreamThread-2"), is(true));
            assertThat(localThreadsNames2.contains("StreamThread-1"), is(true));
            assertThat(localThreadsNames2.contains("StreamThread-2"), is(true));
        } finally {
            CLUSTER.deleteTopicsAndWait(NEW_STREAM);
        }
    }

    @Test
    public void shouldBackOffTaskAndEmitDataWithinSameTopology() throws Exception {
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(DELAYED_INPUT_STREAM_2, 2, 1);

        try {
            final AtomicInteger noOutputExpected = new AtomicInteger(0);
            final AtomicInteger outputExpected = new AtomicInteger(0);
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 15000L);
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

            streams = new KafkaStreamsNamedTopologyWrapper(props);
            streams.setUncaughtExceptionHandler(exception -> StreamThreadExceptionResponse.REPLACE_THREAD);

            final NamedTopologyBuilder builder = streams.newNamedTopologyBuilder("topology_A");
            builder.stream(DELAYED_INPUT_STREAM_1).peek((k, v) -> outputExpected.incrementAndGet()).to(OUTPUT_STREAM_1);
            builder.stream(DELAYED_INPUT_STREAM_2)
                .peek((k, v) -> {
                    throw new RuntimeException("Kaboom");
                })
                .peek((k, v) -> noOutputExpected.incrementAndGet())
                .to(OUTPUT_STREAM_2);

            streams.addNamedTopology(builder.build());

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(streams);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                DELAYED_INPUT_STREAM_2,
                Arrays.asList(
                    new KeyValue<>(1, "A")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                DELAYED_INPUT_STREAM_1,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                OUTPUT_STREAM_1,
                Arrays.asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
            assertThat(noOutputExpected.get(), equalTo(0));
            assertThat(outputExpected.get(), equalTo(2));
        } finally {
            CLUSTER.deleteTopics(DELAYED_INPUT_STREAM_1, DELAYED_INPUT_STREAM_2);
        }
    }

    /**
     * Validates that each metadata object has only partitions & state stores for its specific topology name and
     * asserts that {@code left} and {@code right} differ only by {@link StreamsMetadata#hostInfo()}
     */
    private void verifyMetadataForTopology(final String topologyName,
                                           final Collection<StreamsMetadata> left,
                                           final Collection<StreamsMetadata> right) {
        assertThat(left.size(), equalTo(right.size()));
        final Iterator<StreamsMetadata> leftIter = left.iterator();
        final Iterator<StreamsMetadata> rightIter = right.iterator();

        while (leftIter.hasNext()) {
            final StreamsMetadataImpl leftMetadata = (StreamsMetadataImpl) leftIter.next();
            final StreamsMetadataImpl rightMetadata = (StreamsMetadataImpl) rightIter.next();

            verifyPartitionsAndStoresForTopology(topologyName, leftMetadata);
            verifyPartitionsAndStoresForTopology(topologyName, rightMetadata);

            assertThat(verifyEquivalentMetadataForHost(leftMetadata, rightMetadata), is(true));
        }
    }

    private void verifyPartitionsAndStoresForTopology(final String topologyName, final StreamsMetadataImpl metadata) {
        assertThat(metadata.topologyName(), equalTo(topologyName));
        assertThat(streams.getTopologyByName(topologyName).isPresent(), is(true));
        assertThat(streams2.getTopologyByName(topologyName).isPresent(), is(true));
        final List<String> streams1SourceTopicsForTopology = streams.getTopologyByName(topologyName).get().sourceTopics();
        final List<String> streams2SourceTopicsForTopology = streams2.getTopologyByName(topologyName).get().sourceTopics();

        // first check that all partitions in the metadata correspond to the given named topology
        assertThat(
            streams1SourceTopicsForTopology.containsAll(metadata.topicPartitions().stream()
                                                            .map(TopicPartition::topic)
                                                            .collect(Collectors.toList())),
            is(true));
        assertThat(
            streams2SourceTopicsForTopology.containsAll(metadata.topicPartitions().stream()
                                                            .map(TopicPartition::topic)
                                                            .collect(Collectors.toList())),
            is(true));

        // then verify that only this topology's one store appears if the host has partitions assigned
        if (!metadata.topicPartitions().isEmpty()) {
            assertThat(metadata.stateStoreNames(), equalTo(singleton("store-" + topologyName)));
        } else {
            assertThat(metadata.stateStoreNames().isEmpty(), is(true));
        }

        // finally make sure the standby fields are empty since they are not enabled for this test
        assertThat(metadata.standbyTopicPartitions().isEmpty(), is(true));
        assertThat(metadata.standbyStateStoreNames().isEmpty(), is(true));
    }

    /**
     * @return  true iff all fields other than {@link StreamsMetadataImpl#topologyName()}
     *          match between the two StreamsMetadata objects
     */
    private static boolean verifyEquivalentMetadataForHost(final StreamsMetadataImpl left, final StreamsMetadataImpl right) {
        return left.hostInfo().equals(right.hostInfo())
            && left.stateStoreNames().equals(right.stateStoreNames())
            && left.topicPartitions().equals(right.topicPartitions())
            && left.standbyStateStoreNames().equals(right.standbyStateStoreNames())
            && left.standbyTopicPartitions().equals(right.standbyTopicPartitions());
    }

    private static void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            records,
            producerConfig,
            CLUSTER.time
        );
    }

    private static class TrackingExceptionHandler implements StreamsUncaughtExceptionHandler {
        private final Map<String, Queue<Throwable>> newErrorsByTopology = new HashMap<>();

        @Override
        public synchronized StreamThreadExceptionResponse handle(final Throwable exception) {
            final String topologyName =
                exception instanceof StreamsException && ((StreamsException) exception).taskId().isPresent() ?
                    ((StreamsException) exception).taskId().get().topologyName()
                    : null;

            newErrorsByTopology.computeIfAbsent(topologyName, t -> new LinkedList<>()).add(exception);
            if (exception.getCause() instanceof MissingSourceTopicException) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            } else {
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
        }

        public synchronized Throwable nextError(final String topologyName) {
            return newErrorsByTopology.containsKey(topologyName) ?
                newErrorsByTopology.get(topologyName).poll() :
                null;
        }
    }
}
