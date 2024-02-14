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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreamsWrapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class LagFetchIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private static final long WAIT_TIMEOUT_MS = 120000;
    private static final Logger LOG = LoggerFactory.getLogger(LagFetchIntegrationTest.class);

    private final MockTime mockTime = CLUSTER.time;
    private Properties streamsConfiguration;
    private Properties consumerConfiguration;
    private String inputTopicName;
    private String outputTopicName;
    private String stateStoreName;

    @BeforeEach
    public void before(final TestInfo testInfo) {
        final String safeTestName = safeUniqueTestName(testInfo);
        inputTopicName = "input-topic-" + safeTestName;
        outputTopicName = "output-topic-" + safeTestName;
        stateStoreName = "lagfetch-test-store" + safeTestName;

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

        consumerConfiguration = new Properties();
        consumerConfiguration.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfiguration.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerConfiguration.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfiguration.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfiguration.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    }

    @AfterEach
    public void shutdown() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    private Map<String, Map<Integer, LagInfo>> getFirstNonEmptyLagMap(final KafkaStreams streams) throws InterruptedException {
        final Map<String, Map<Integer, LagInfo>> offsetLagInfoMap = new HashMap<>();
        TestUtils.waitForCondition(() -> {
            final Map<String, Map<Integer, LagInfo>> lagMap = streams.allLocalStorePartitionLags();
            if (lagMap.size() > 0) {
                offsetLagInfoMap.putAll(lagMap);
            }
            return lagMap.size() > 0;
        }, WAIT_TIMEOUT_MS, "Should obtain non-empty lag information eventually");
        return offsetLagInfoMap;
    }

    private void shouldFetchLagsDuringRebalancing(final String optimization) throws Exception {
        final CountDownLatch latchTillActiveIsRunning = new CountDownLatch(1);
        final CountDownLatch latchTillStandbyIsRunning = new CountDownLatch(1);
        final CountDownLatch latchTillStandbyHasPartitionsAssigned = new CountDownLatch(1);
        final CyclicBarrier lagCheckBarrier = new CyclicBarrier(2);
        final List<KafkaStreamsWrapper> streamsList = new ArrayList<>();

        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopicName,
            mkSet(new KeyValue<>("k1", 1L), new KeyValue<>("k2", 2L), new KeyValue<>("k3", 3L), new KeyValue<>("k4", 4L), new KeyValue<>("k5", 5L)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                LongSerializer.class,
                new Properties()),
            mockTime);

        // create stream threads
        for (int i = 0; i < 2; i++) {
            final Properties props = (Properties) streamsConfiguration.clone();
            // this test relies on the second instance getting the standby, so we specify
            // an assignor with this contract.
            props.put(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, FallbackPriorTaskAssignor.class.getName());
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + i);
            props.put(StreamsConfig.CLIENT_ID_CONFIG, "instance-" + i);
            props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(stateStoreName + i).getAbsolutePath());

            final StreamsBuilder builder = new StreamsBuilder();
            final KTable<String, Long> t1 = builder.table(inputTopicName, Materialized.as(stateStoreName));
            t1.toStream().to(outputTopicName);
            final KafkaStreamsWrapper streams = new KafkaStreamsWrapper(builder.build(props), props);
            streamsList.add(streams);
        }

        final KafkaStreamsWrapper activeStreams = streamsList.get(0);
        final KafkaStreamsWrapper standbyStreams = streamsList.get(1);
        activeStreams.setStreamThreadStateListener((thread, newState, oldState) -> {
            if (newState == StreamThread.State.RUNNING) {
                latchTillActiveIsRunning.countDown();
            }
        });
        standbyStreams.setStreamThreadStateListener((thread, newState, oldState) -> {
            if (oldState == StreamThread.State.PARTITIONS_ASSIGNED && newState == StreamThread.State.RUNNING) {
                latchTillStandbyHasPartitionsAssigned.countDown();
                try {
                    lagCheckBarrier.await(60, TimeUnit.SECONDS);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (newState == StreamThread.State.RUNNING) {
                latchTillStandbyIsRunning.countDown();
            }
        });

        try {
            // First start up the active.
            TestUtils.waitForCondition(() -> activeStreams.allLocalStorePartitionLags().size() == 0,
                WAIT_TIMEOUT_MS,
                "Should see empty lag map before streams is started.");
            activeStreams.start();
            latchTillActiveIsRunning.await(60, TimeUnit.SECONDS);

            IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                consumerConfiguration,
                outputTopicName,
                5,
                WAIT_TIMEOUT_MS);
            // Check the active reports proper lag values.
            Map<String, Map<Integer, LagInfo>> offsetLagInfoMap = getFirstNonEmptyLagMap(activeStreams);
            assertThat(offsetLagInfoMap.size(), equalTo(1));
            assertThat(offsetLagInfoMap.keySet(), equalTo(mkSet(stateStoreName)));
            assertThat(offsetLagInfoMap.get(stateStoreName).size(), equalTo(1));
            LagInfo lagInfo = offsetLagInfoMap.get(stateStoreName).get(0);
            assertThat(lagInfo.currentOffsetPosition(), equalTo(5L));
            assertThat(lagInfo.endOffsetPosition(), equalTo(5L));
            assertThat(lagInfo.offsetLag(), equalTo(0L));

            // start up the standby & make it pause right after it has partition assigned
            standbyStreams.start();
            latchTillStandbyHasPartitionsAssigned.await(60, TimeUnit.SECONDS);
            offsetLagInfoMap = getFirstNonEmptyLagMap(standbyStreams);
            assertThat(offsetLagInfoMap.size(), equalTo(1));
            assertThat(offsetLagInfoMap.keySet(), equalTo(mkSet(stateStoreName)));
            assertThat(offsetLagInfoMap.get(stateStoreName).size(), equalTo(1));
            lagInfo = offsetLagInfoMap.get(stateStoreName).get(0);
            assertThat(lagInfo.currentOffsetPosition(), equalTo(0L));
            assertThat(lagInfo.endOffsetPosition(), equalTo(5L));
            assertThat(lagInfo.offsetLag(), equalTo(5L));
            // standby thread wont proceed to RUNNING before this barrier is crossed
            lagCheckBarrier.await(60, TimeUnit.SECONDS);

            // wait till the lag goes down to 0, on the standby
            TestUtils.waitForCondition(() -> standbyStreams.allLocalStorePartitionLags().get(stateStoreName).get(0).offsetLag() == 0,
                WAIT_TIMEOUT_MS,
                "Standby should eventually catchup and have zero lag.");
        } finally {
            for (final KafkaStreams streams : streamsList) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldFetchLagsDuringRebalancingWithOptimization() throws Exception {
        shouldFetchLagsDuringRebalancing(StreamsConfig.OPTIMIZE);
    }

    @Test
    public void shouldFetchLagsDuringRebalancingWithNoOptimization() throws Exception {
        shouldFetchLagsDuringRebalancing(StreamsConfig.NO_OPTIMIZATION);
    }

    @Test
    public void shouldFetchLagsDuringRestoration() throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopicName,
            mkSet(new KeyValue<>("k1", 1L), new KeyValue<>("k2", 2L), new KeyValue<>("k3", 3L), new KeyValue<>("k4", 4L), new KeyValue<>("k5", 5L)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                LongSerializer.class,
                new Properties()),
            mockTime);

        // create stream threads
        final Properties props = (Properties) streamsConfiguration.clone();
        final File stateDir = TestUtils.tempDirectory(stateStoreName + "0");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:0");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "instance-0");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> t1 = builder.table(inputTopicName, Materialized.as(stateStoreName));
        t1.toStream().to(outputTopicName);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final AtomicReference<LagInfo> zeroLagRef = new AtomicReference<>();
        try {
            // First start up the active.
            TestUtils.waitForCondition(() -> streams.allLocalStorePartitionLags().size() == 0,
                WAIT_TIMEOUT_MS,
                "Should see empty lag map before streams is started.");

            // Get the instance to fully catch up and reach RUNNING state
            startApplicationAndWaitUntilRunning(streams);
            IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                consumerConfiguration,
                outputTopicName,
                5,
                WAIT_TIMEOUT_MS);

            // check for proper lag values.
            TestUtils.waitForCondition(() -> {
                final Map<String, Map<Integer, LagInfo>> offsetLagInfoMap = streams.allLocalStorePartitionLags();
                assertThat(offsetLagInfoMap.size(), equalTo(1));
                assertThat(offsetLagInfoMap.keySet(), equalTo(mkSet(stateStoreName)));
                assertThat(offsetLagInfoMap.get(stateStoreName).size(), equalTo(1));

                final LagInfo zeroLagInfo = offsetLagInfoMap.get(stateStoreName).get(0);
                assertThat(zeroLagInfo.currentOffsetPosition(), equalTo(5L));
                assertThat(zeroLagInfo.endOffsetPosition(), equalTo(5L));
                assertThat(zeroLagInfo.offsetLag(), equalTo(0L));
                zeroLagRef.set(zeroLagInfo);
                return true;
            }, WAIT_TIMEOUT_MS, "Eventually should reach zero lag.");

            // Kill instance, delete state to force restoration.
            assertThat("Streams instance did not close within timeout", streams.close(Duration.ofSeconds(60)));
            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
            Files.walk(stateDir.toPath()).sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(f -> assertTrue(f.delete(), "Some state " + f + " could not be deleted"));
        } finally {
            streams.close();
            streams.cleanUp();
        }

        // wait till the lag goes down to 0
        final KafkaStreams restartedStreams = new KafkaStreams(builder.build(), props);
        try {
            // set a state restoration listener to track progress of restoration
            final CountDownLatch restorationEndLatch = new CountDownLatch(1);
            final Map<String, Map<Integer, LagInfo>> restoreStartLagInfo = new HashMap<>();
            final Map<String, Map<Integer, LagInfo>> restoreEndLagInfo = new HashMap<>();
            restartedStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(final TopicPartition topicPartition, final String storeName, final long startingOffset, final long endingOffset) {
                    try {
                        restoreStartLagInfo.putAll(getFirstNonEmptyLagMap(restartedStreams));
                    } catch (final Exception e) {
                        LOG.error("Exception while trying to obtain lag map", e);
                    }
                }

                @Override
                public void onBatchRestored(final TopicPartition topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {
                }

                @Override
                public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
                    try {
                        restoreEndLagInfo.putAll(getFirstNonEmptyLagMap(restartedStreams));
                    } catch (final Exception e) {
                        LOG.error("Exception while trying to obtain lag map", e);
                    }
                    restorationEndLatch.countDown();
                }
            });

            restartedStreams.start();
            restorationEndLatch.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            TestUtils.waitForCondition(() -> restartedStreams.allLocalStorePartitionLags().get(stateStoreName).get(0).offsetLag() == 0,
                WAIT_TIMEOUT_MS,
                "Standby should eventually catchup and have zero lag.");
            final LagInfo fullLagInfo = restoreStartLagInfo.get(stateStoreName).get(0);
            assertThat(fullLagInfo.currentOffsetPosition(), equalTo(0L));
            assertThat(fullLagInfo.endOffsetPosition(), equalTo(5L));
            assertThat(fullLagInfo.offsetLag(), equalTo(5L));

            assertThat(restoreEndLagInfo.get(stateStoreName).get(0), equalTo(zeroLagRef.get()));
        } finally {
            restartedStreams.close();
            restartedStreams.cleanUp();
        }
    }
}
