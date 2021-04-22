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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

/**
 * An integration test to verify the conversion of a dirty-closed EOS
 * task towards a standby task is safe across restarts of the application.
 */
@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class StandbyTaskEOSIntegrationTest {

    private final static long REBALANCE_TIMEOUT = Duration.ofMinutes(2L).toMillis();
    private final static int KEY_0 = 0;
    private final static int KEY_1 = 1;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return asList(new String[][] {
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_BETA}
        });
    }

    @Parameterized.Parameter
    public String eosConfig;

    private final AtomicBoolean skipRecord = new AtomicBoolean(false);

    private String appId;
    private String inputTopic;
    private String storeName;
    private String outputTopic;

    private KafkaStreams streamInstanceOne;
    private KafkaStreams streamInstanceTwo;
    private KafkaStreams streamInstanceOneRecovery;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void createTopics() throws Exception {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        appId = "app-" + safeTestName;
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        storeName = "store-" + safeTestName;
        CLUSTER.deleteTopicsAndWait(inputTopic, outputTopic, appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog");
        CLUSTER.createTopic(inputTopic, 1, 3);
        CLUSTER.createTopic(outputTopic, 1, 3);
    }

    @After
    public void cleanUp() {
        if (streamInstanceOne != null) {
            streamInstanceOne.close();
        }
        if (streamInstanceTwo != null) {
            streamInstanceTwo.close();
        }
        if (streamInstanceOneRecovery != null) {
            streamInstanceOneRecovery.close();
        }
    }

    @Test
    public void shouldSurviveWithOneTaskAsStandby() throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(0, 0)
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            10L
        );

        final String stateDirPath = TestUtils.tempDirectory(appId).getPath();

        final CountDownLatch instanceLatch = new CountDownLatch(1);

        streamInstanceOne = buildStreamWithDirtyStateDir(stateDirPath + "/" + appId + "-1/", instanceLatch);
        streamInstanceTwo = buildStreamWithDirtyStateDir(stateDirPath + "/" + appId + "-2/", instanceLatch);

        startApplicationAndWaitUntilRunning(asList(streamInstanceOne, streamInstanceTwo), Duration.ofSeconds(60));

        // Wait for the record to be processed
        assertTrue(instanceLatch.await(15, TimeUnit.SECONDS));

        streamInstanceOne.close(Duration.ZERO);
        streamInstanceTwo.close(Duration.ZERO);

        streamInstanceOne.cleanUp();
        streamInstanceTwo.cleanUp();
    }

    private KafkaStreams buildStreamWithDirtyStateDir(final String stateDirPath,
                                                      final CountDownLatch recordProcessLatch) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();
        final TaskId taskId = new TaskId(0, 0);

        final Properties props = props(stateDirPath);

        final StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(props), new MockTime(), true);

        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(taskId), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition("unknown-topic", 0), 5L));

        assertTrue(new File(stateDirectory.getOrCreateDirectoryForTask(taskId),
                            "rocksdb/KSTREAM-AGGREGATE-STATE-STORE-0000000001").mkdirs());

        builder.stream(inputTopic,
                       Consumed.with(Serdes.Integer(), Serdes.Integer()))
               .groupByKey()
               .count()
               .toStream()
               .peek((key, value) -> recordProcessLatch.countDown());

        return new KafkaStreams(builder.build(), props);
    }

    @Test
    public void shouldWipeOutStandbyStateDirectoryIfCheckpointIsMissing() throws Exception {
        final long time = System.currentTimeMillis();
        final String base = TestUtils.tempDirectory(appId).getPath();

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(KEY_0, 0)
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            10L + time
        );

        streamInstanceOne = buildWithDeduplicationTopology(base + "-1");
        streamInstanceTwo = buildWithDeduplicationTopology(base + "-2");

        // start first instance and wait for processing
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceOne), Duration.ofSeconds(30));
        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class
            ),
            outputTopic,
            1
        );

        // start second instance and wait for standby replication
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceTwo), Duration.ofSeconds(30));
        waitForCondition(
            () -> streamInstanceTwo.store(
                StoreQueryParameters.fromNameAndType(
                    storeName,
                    QueryableStoreTypes.<Integer, Integer>keyValueStore()
                ).enableStaleStores()
            ).get(KEY_0) != null,
            REBALANCE_TIMEOUT,
            "Could not get key from standby store"
        );
        // sanity check that first instance is still active
        waitForCondition(
            () -> streamInstanceOne.store(
                StoreQueryParameters.fromNameAndType(
                    storeName,
                    QueryableStoreTypes.<Integer, Integer>keyValueStore()
                )
            ).get(KEY_0) != null,
            "Could not get key from main store"
        );

        // inject poison pill and wait for crash of first instance and recovery on second instance
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(KEY_1, 0)
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            10L + time
        );
        waitForCondition(
            () -> streamInstanceOne.state() == KafkaStreams.State.ERROR,
            "Stream instance 1 did not go into error state"
        );
        streamInstanceOne.close();

        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                IntegerDeserializer.class,
                IntegerDeserializer.class
            ),
            outputTopic,
            2
        );

        streamInstanceOneRecovery = buildWithDeduplicationTopology(base + "-1");

        // "restart" first client and wait for standby recovery
        // (could actually also be active, but it does not matter as long as we enable "state stores"
        startApplicationAndWaitUntilRunning(
            Collections.singletonList(streamInstanceOneRecovery),
            Duration.ofSeconds(30)
        );
        waitForCondition(
            () -> streamInstanceOneRecovery.store(
                StoreQueryParameters.fromNameAndType(
                    storeName,
                    QueryableStoreTypes.<Integer, Integer>keyValueStore()
                ).enableStaleStores()
            ).get(KEY_0) != null,
            "Could not get key from recovered standby store"
        );

        streamInstanceTwo.close();
        waitForCondition(
            () -> streamInstanceOneRecovery.store(
                StoreQueryParameters.fromNameAndType(
                    storeName,
                    QueryableStoreTypes.<Integer, Integer>keyValueStore()
                )
            ).get(KEY_0) != null,
            REBALANCE_TIMEOUT,
            "Could not get key from recovered main store"
        );

        // re-inject poison pill and wait for crash of first instance
        skipRecord.set(false);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(KEY_1, 0)
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            10L + time
        );
        waitForCondition(
            () -> streamInstanceOneRecovery.state() == KafkaStreams.State.ERROR,
            "Stream instance 1 did not go into error state. Is in " + streamInstanceOneRecovery.state() + " state."
        );
    }

    private KafkaStreams buildWithDeduplicationTopology(final String stateDirPath) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.Integer(),
            Serdes.Integer())
        );
        builder.<Integer, Integer>stream(inputTopic)
            .transform(
                () -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
                    private KeyValueStore<Integer, Integer> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext context) {
                        store = (KeyValueStore<Integer, Integer>) context.getStateStore(storeName);
                    }

                    @Override
                    public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                        if (skipRecord.get()) {
                            // we only forward so we can verify the skipping by reading the output topic
                            // the goal is skipping is to not modify the state store
                            return KeyValue.pair(key, value);
                        }

                        if (store.get(key) != null) {
                            return null;
                        }

                        store.put(key, value);
                        store.flush();

                        if (key == KEY_1) {
                            // after error injection, we need to avoid a consecutive error after rebalancing
                            skipRecord.set(true);
                            throw new RuntimeException("Injected test error");
                        }

                        return KeyValue.pair(key, value);
                    }

                    @Override
                    public void close() { }
                },
                storeName
            )
            .to(outputTopic);

        return new KafkaStreams(builder.build(), props(stateDirPath));
    }


    private Properties props(final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // need to set to zero to get predictable active/standby task assignments
        streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
