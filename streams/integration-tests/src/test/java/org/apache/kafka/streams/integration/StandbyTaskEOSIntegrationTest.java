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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An integration test to verify the conversion of a dirty-closed EOS
 * task towards a standby task is safe across restarts of the application.
 */
@Tag("integration")
@Timeout(600)
public class StandbyTaskEOSIntegrationTest {
    private static final long REBALANCE_TIMEOUT = Duration.ofMinutes(2L).toMillis();
    private static final int KEY_0 = 0;
    private static final int KEY_1 = 1;

    private final AtomicBoolean skipRecord = new AtomicBoolean(false);

    private String appId;
    private String inputTopic;
    private String storeName;
    private String outputTopic;

    private KafkaStreams streamInstanceOne;
    private KafkaStreams streamInstanceTwo;
    private KafkaStreams streamInstanceOneRecovery;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        final String safeTestName = safeUniqueTestName(testInfo);
        appId = "app-" + safeTestName;
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        storeName = "store-" + safeTestName;
        CLUSTER.deleteTopics(inputTopic, outputTopic, appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog");
        CLUSTER.createTopic(inputTopic, 1, 3);
        CLUSTER.createTopic(outputTopic, 1, 3);
    }

    @AfterEach
    public void cleanUp() {
        if (streamInstanceOne != null) {
            streamInstanceOne.close(Duration.ofSeconds(60));
        }
        if (streamInstanceTwo != null) {
            streamInstanceTwo.close(Duration.ofSeconds(60));
        }
        if (streamInstanceOneRecovery != null) {
            streamInstanceOneRecovery.close(Duration.ofSeconds(60));
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

        streamInstanceOne.close();
        streamInstanceTwo.close();

        streamInstanceOne.cleanUp();
        streamInstanceTwo.cleanUp();
    }

    private KafkaStreams buildStreamWithDirtyStateDir(final String stateDirPath,
                                                      final CountDownLatch recordProcessLatch) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();
        final TaskId taskId = new TaskId(0, 0);

        final Properties props = props(stateDirPath);

        final StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(props), new MockTime(), true, false);

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
        startApplicationAndWaitUntilRunning(streamInstanceOne);
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
        startApplicationAndWaitUntilRunning(streamInstanceTwo);
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
        startApplicationAndWaitUntilRunning(streamInstanceOneRecovery);
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
            .process(
                () -> new Processor<Integer, Integer, Integer, Integer>() {
                    private ProcessorContext<Integer, Integer> context;
                    private KeyValueStore<Integer, Integer> store;

                    @Override
                    public void init(final ProcessorContext<Integer, Integer> context) {
                        this.context = context;
                        store = context.getStateStore(storeName);
                    }

                    @Override
                    public void process(final Record<Integer, Integer> record) {
                        final int key = record.key();
                        final int value = record.value();

                        if (skipRecord.get()) {
                            // we only forward so we can verify the skipping by reading the output topic
                            // the goal is skipping is to not modify the state store
                            context.forward(record);
                            return;
                        }

                        if (store.get(key) != null) {
                            return;
                        }

                        store.put(key, value);
                        store.flush();

                        if (key == KEY_1) {
                            // after error injection, we need to avoid a consecutive error after rebalancing
                            skipRecord.set(true);
                            throw new RuntimeException("Injected test error");
                        }

                        context.forward(record);
                    }
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
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        // need to set to zero to get predictable active/standby task assignments
        streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
