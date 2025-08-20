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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStore;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(600)
@Tag("integration")
public class StoreQueryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(StoreQueryIntegrationTest.class);

    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private String appId;

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = CLUSTER.time;

    @BeforeAll
    public static void setupCluster() throws InterruptedException, IOException {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException, IOException {
        this.appId = safeUniqueTestName(testInfo);
    }

    @AfterEach
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close(Duration.ofSeconds(60));
        }
        streamsToCleanup.clear();
    }

    @AfterAll
    public static void stopCluster() {
        CLUSTER.stop();
    }

    @Test
    public void shouldQueryOnlyActivePartitionStoresByDefault() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final int kafkaStreams1Port = port;
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        until(() -> {

            final KeyQueryMetadata keyQueryMetadata = kafkaStreams1
                    .queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> Optional.of(Collections.singleton(0)));

            final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(TABLE_NAME, kafkaStreams1, queryableStoreType);
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(TABLE_NAME, kafkaStreams2, queryableStoreType);

            final boolean kafkaStreams1IsActive = keyQueryMetadata.activeHost().port() == kafkaStreams1Port;

            try {
                if (kafkaStreams1IsActive) {
                    assertThat(store1.get(key), is(notNullValue()));
                    assertThat(store2.get(key), is(nullValue()));
                } else {
                    assertThat(store1.get(key), is(nullValue()));
                    assertThat(store2.get(key), is(notNullValue()));
                }
                return true;
            } catch (final InvalidStateStoreException exception) {
                verifyRetriableException(exception);
                LOG.info("Either streams wasn't running or a re-balancing took place. Will try again.");
                return false;
            }
        });
    }

    @Test
    public void shouldQuerySpecificActivePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final int kafkaStreams1Port = port;
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        until(() -> {
            final KeyQueryMetadata keyQueryMetadata = kafkaStreams1
                    .queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> Optional.of(Collections.singleton(0)));

            //key belongs to this partition
            final int keyPartition = keyQueryMetadata.partition();

            //key doesn't belongs to this partition
            final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
            final boolean kafkaStreams1IsActive = keyQueryMetadata.activeHost().port() == kafkaStreams1Port;

            final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> storeQueryParam =
                StoreQueryParameters.<ReadOnlyKeyValueStore<Integer, Integer>>fromNameAndType(TABLE_NAME, keyValueStore())
                    .withPartition(keyPartition);
            ReadOnlyKeyValueStore<Integer, Integer> store1 = null;
            ReadOnlyKeyValueStore<Integer, Integer> store2 = null;
            if (kafkaStreams1IsActive) {
                store1 = getStore(kafkaStreams1, storeQueryParam);
            } else {
                store2 = getStore(kafkaStreams2, storeQueryParam);
            }

            if (kafkaStreams1IsActive) {
                assertThat(store1, is(notNullValue()));
                assertThat(store2, is(nullValue()));
            } else {
                assertThat(store2, is(notNullValue()));
                assertThat(store1, is(nullValue()));
            }

            final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> storeQueryParam2 =
                StoreQueryParameters.<ReadOnlyKeyValueStore<Integer, Integer>>fromNameAndType(TABLE_NAME, keyValueStore())
                .withPartition(keyDontBelongPartition);

            try {
                // Assert that key is not served when wrong specific partition is requested
                // If kafkaStreams1 is active for keyPartition, kafkaStreams2 would be active for keyDontBelongPartition
                // So, in that case, store3 would be null and the store4 would not return the value for key as wrong partition was requested
                if (kafkaStreams1IsActive) {
                    assertThat(store1.get(key), is(notNullValue()));
                    assertThat(getStore(kafkaStreams2, storeQueryParam2).get(key), is(nullValue()));
                    final InvalidStateStoreException exception =
                        assertThrows(InvalidStateStoreException.class, () -> getStore(kafkaStreams1, storeQueryParam2).get(key));
                    assertThat(
                        exception.getMessage(),
                        containsString("The specified partition 1 for store source-table does not exist.")
                    );
                } else {
                    assertThat(store2.get(key), is(notNullValue()));
                    assertThat(getStore(kafkaStreams1, storeQueryParam2).get(key), is(nullValue()));
                    final InvalidStateStoreException exception =
                        assertThrows(InvalidStateStoreException.class, () -> getStore(kafkaStreams2, storeQueryParam2).get(key));
                    assertThat(
                        exception.getMessage(),
                        containsString("The specified partition 1 for store source-table does not exist.")
                    );
                }
                return true;
            } catch (final InvalidStateStoreException exception) {
                verifyRetriableException(exception);
                LOG.info("Either streams wasn't running or a re-balancing took place. Will try again.");
                return false;
            }
        });
    }

    @Test
    public void shouldQueryAllStalePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();

        // Assert that both active and standby are able to query for a key
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(TABLE_NAME, kafkaStreams1, true, queryableStoreType);
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(TABLE_NAME, kafkaStreams2, true, queryableStoreType);
            return store2.get(key) != null;
        }, "store2 cannot find results for key");
    }

    @Test
    public void shouldQuerySpecificStalePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1
                .queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> Optional.of(Collections.singleton(0)));

        //key belongs to this partition
        final int keyPartition = keyQueryMetadata.partition();

        //key doesn't belongs to this partition
        final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();

        // Assert that both active and standby are able to query for a key
        final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> param = StoreQueryParameters
            .fromNameAndType(TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyPartition);
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(kafkaStreams1, param);
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(kafkaStreams2, param);
            return store2.get(key) != null;
        }, "store2 cannot find results for key");

        final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> otherParam = StoreQueryParameters
            .fromNameAndType(TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyDontBelongPartition);
        final ReadOnlyKeyValueStore<Integer, Integer> store3 = getStore(kafkaStreams1, otherParam);
        final ReadOnlyKeyValueStore<Integer, Integer> store4 = getStore(kafkaStreams2, otherParam);

        // Assert that
        assertThat(store3.get(key), is(nullValue()));
        assertThat(store4.get(key), is(nullValue()));
    }

    @Test
    public void shouldQuerySpecificStalePartitionStoresMultiStreamThreads() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);
        final int numStreamThreads = 2;

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final Properties streamsConfiguration1 = streamsConfiguration();
        streamsConfiguration1.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);

        final Properties streamsConfiguration2 = streamsConfiguration();
        streamsConfiguration2.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration1);
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration2);
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        assertThat(kafkaStreams1.metadataForLocalThreads().size(), greaterThan(1));
        assertThat(kafkaStreams2.metadataForLocalThreads().size(), greaterThan(1));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, new IntegerSerializer());

        //key belongs to this partition
        final int keyPartition = keyQueryMetadata.partition();

        //key doesn't belongs to this partition
        final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();

        // Assert that both active and standby are able to query for a key
        final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> param = StoreQueryParameters
            .fromNameAndType(TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyPartition);
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(kafkaStreams1, param);
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(kafkaStreams2, param);
            return store2.get(key) != null;
        }, "store2 cannot find results for key");

        final StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> otherParam = StoreQueryParameters
            .fromNameAndType(TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyDontBelongPartition);
        final ReadOnlyKeyValueStore<Integer, Integer> store3 = getStore(kafkaStreams1, otherParam);
        final ReadOnlyKeyValueStore<Integer, Integer> store4 = getStore(kafkaStreams2, otherParam);

        // Assert that
        assertThat(store3.get(key), is(nullValue()));
        assertThat(store4.get(key), is(nullValue()));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldQuerySpecificStalePartitionStoresMultiStreamThreadsNamedTopology() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);
        final int numStreamThreads = 2;

        final Properties streamsConfiguration1 = streamsConfiguration();
        streamsConfiguration1.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);

        final Properties streamsConfiguration2 = streamsConfiguration();
        streamsConfiguration2.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);

        final String topologyA = "topology-A";

        final KafkaStreamsNamedTopologyWrapper kafkaStreams1 = createNamedTopologyKafkaStreams(streamsConfiguration1);
        final KafkaStreamsNamedTopologyWrapper kafkaStreams2 = createNamedTopologyKafkaStreams(streamsConfiguration2);
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        final NamedTopologyBuilder builder1A = kafkaStreams1.newNamedTopologyBuilder(topologyA, streamsConfiguration1);
        getStreamsBuilderWithTopology(builder1A, semaphore);

        final NamedTopologyBuilder builder2A = kafkaStreams2.newNamedTopologyBuilder(topologyA, streamsConfiguration2);
        getStreamsBuilderWithTopology(builder2A, semaphore);

        kafkaStreams1.start(builder1A.build());
        kafkaStreams2.start(builder2A.build());
        waitForApplicationState(kafkaStreamsList, State.RUNNING, Duration.ofSeconds(60));

        assertThat(kafkaStreams1.metadataForLocalThreads().size(), greaterThan(1));
        assertThat(kafkaStreams2.metadataForLocalThreads().size(), greaterThan(1));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, new IntegerSerializer(), topologyA);

        //key belongs to this partition
        final int keyPartition = keyQueryMetadata.partition();

        //key doesn't belongs to this partition
        final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();

        // Assert that both active and standby are able to query for a key
        final NamedTopologyStoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> param = NamedTopologyStoreQueryParameters
            .fromNamedTopologyAndStoreNameAndType(topologyA, TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyPartition);
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(kafkaStreams1, param);
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = getStore(kafkaStreams2, param);
            return store2.get(key) != null;
        }, "store2 cannot find results for key");

        final NamedTopologyStoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> otherParam = NamedTopologyStoreQueryParameters
            .fromNamedTopologyAndStoreNameAndType(topologyA, TABLE_NAME, queryableStoreType)
            .enableStaleStores()
            .withPartition(keyDontBelongPartition);
        final ReadOnlyKeyValueStore<Integer, Integer> store3 = getStore(kafkaStreams1, otherParam);
        final ReadOnlyKeyValueStore<Integer, Integer> store4 = getStore(kafkaStreams2, otherParam);

        // Assert that
        assertThat(store3.get(key), is(nullValue()));
        assertThat(store4.get(key), is(nullValue()));
    }

    @Test
    public void shouldQueryStoresAfterAddingAndRemovingStreamThread() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final int key2 = 2;
        final int key3 = 3;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final Properties streamsConfiguration1 = streamsConfiguration();
        streamsConfiguration1.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration1);

        startApplicationAndWaitUntilRunning(singletonList(kafkaStreams1), Duration.ofSeconds(60));
        //Add thread
        final Optional<String> streamThread = kafkaStreams1.addStreamThread();
        assertThat(streamThread.isPresent(), is(true));
        until(() -> kafkaStreams1.state().isRunningOrRebalancing());

        produceValueRange(key, 0, batch1NumMessages);
        produceValueRange(key2, 0, batch1NumMessages);
        produceValueRange(key3, 0, batch1NumMessages);

        // Assert that all messages in the batches were processed in a timely manner
        assertThat(semaphore.tryAcquire(3 * batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        until(() -> KafkaStreams.State.RUNNING.equals(kafkaStreams1.state()));
        until(() -> {
            final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(TABLE_NAME, kafkaStreams1, queryableStoreType);

            try {
                assertThat(store1.get(key), is(notNullValue()));
                assertThat(store1.get(key2), is(notNullValue()));
                assertThat(store1.get(key3), is(notNullValue()));
                return true;
            } catch (final InvalidStateStoreException exception) {
                verifyRetriableException(exception);
                LOG.info("Either streams wasn't running or a re-balancing took place. Will try again.");
                return false;
            }
        });

        final Optional<String> removedThreadName = kafkaStreams1.removeStreamThread();
        assertThat(removedThreadName.isPresent(), is(true));
        until(() -> kafkaStreams1.state().isRunningOrRebalancing());

        until(() -> KafkaStreams.State.RUNNING.equals(kafkaStreams1.state()));
        until(() -> {
            final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = keyValueStore();
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = getStore(TABLE_NAME, kafkaStreams1, queryableStoreType);

            try {
                assertThat(store1.get(key), is(notNullValue()));
                assertThat(store1.get(key2), is(notNullValue()));
                assertThat(store1.get(key3), is(notNullValue()));
                return true;
            } catch (final InvalidStateStoreException exception) {
                verifyRetriableException(exception);
                LOG.info("Either streams wasn't running or a re-balancing took place. Will try again.");
                return false;
            }
        });
    }

    @Test
    public void shouldFailWithIllegalArgumentExceptionWhenIQPartitionerReturnsMultiplePartitions() throws Exception {

        class BroadcastingPartitioner implements StreamPartitioner<Integer, String> {
            @Override
            public Optional<Set<Integer>> partitions(final String topic, final Integer key, final String value, final int numPartitions) {
                return Optional.of(IntStream.range(0, numPartitions).boxed().collect(Collectors.toSet()));
            }
        }

        final int batch1NumMessages = 1;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        getStreamsBuilderWithTopology(builder, semaphore);

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());

        startApplicationAndWaitUntilRunning(Collections.singletonList(kafkaStreams1), Duration.ofSeconds(60));
        produceValueRange(key, 0, batch1NumMessages);

        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        assertThrows(IllegalArgumentException.class, () -> kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, new BroadcastingPartitioner()));
    }


    private Matcher<String> retriableException() {
        return is(
            anyOf(
                containsString("Cannot get state store source-table because the stream thread is PARTITIONS_ASSIGNED, not RUNNING"),
                containsString("The state store, source-table, may have migrated to another instance"),
                containsString("Cannot get state store source-table because the stream thread is STARTING, not RUNNING"),
                containsString("The specified partition 1 for store source-table does not exist.")
            )
        );
    }

    private void verifyRetriableException(final Exception exception) {
        assertThat(
            "Unexpected exception thrown while getting the value from store.",
            exception.getMessage(),
            retriableException()
        );
    }

    private static void until(final TestCondition condition) {
        boolean success = false;
        final long deadline = System.currentTimeMillis() + IntegrationTestUtils.DEFAULT_TIMEOUT;
        boolean deadlineExceeded = System.currentTimeMillis() >= deadline;
        while (!success && !deadlineExceeded) {
            try {
                success = condition.conditionMet();
                Thread.sleep(500L);
            } catch (final RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            } finally {
                deadlineExceeded = System.currentTimeMillis() >= deadline;
            }
        }
        if (deadlineExceeded) {
            fail("Test execution timed out");
        }
    }

    private void getStreamsBuilderWithTopology(final StreamsBuilder builder, final Semaphore semaphore) {
        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
            Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME).withCachingDisabled())
            .toStream()
            .peek((k, v) -> semaphore.release());
    }

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    @SuppressWarnings("deprecation")
    private KafkaStreamsNamedTopologyWrapper createNamedTopologyKafkaStreams(final Properties config) {
        final KafkaStreamsNamedTopologyWrapper streams = new KafkaStreamsNamedTopologyWrapper(config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC_NAME,
            IntStream.range(start, endExclusive)
                     .mapToObj(i -> KeyValue.pair(key, i))
                     .collect(Collectors.toList()),
            producerProps,
            mockTime);
    }

    private Properties streamsConfiguration() {
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + appId);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        return config;
    }
}
