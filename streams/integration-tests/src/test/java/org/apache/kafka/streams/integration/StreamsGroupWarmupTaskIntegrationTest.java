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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streams group protocol (KIP-1071) analog of {@link HighAvailabilityTaskAssignorIntegrationTest}: proves that
 * scaling out a stateful app gets the new instance a warm-up task first, and only promotes it to active once
 * caught up, reusing the warm-up's local state with no restoration.
 *
 * Unlike the classic protocol, warm-up derivation for streams groups happens broker-side via an
 * {@code AssignmentRefiner} that is a {@code NoOp} by default, so the real {@link AssignmentRefinerImpl} is
 * enabled explicitly via {@code group.streams.assignment.refiner.class}. That config is broker-internal
 * (its own doc string says "This should be used for testing only") because warm-up derivation isn't wired
 * in as a production default yet; this test exercises the mechanism directly instead of waiting for that.
 */
@Timeout(600)
@Tag("integration")
public class StreamsGroupWarmupTaskIntegrationTest {
    private static final Properties BROKER_CONFIG = mkProperties(mkMap(
        mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG, AssignmentRefinerImpl.class.getName()),
        mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_CONFIG, "0")
    ));

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3, BROKER_CONFIG);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Test
    public void shouldScaleOutWithWarmupTasksAndInMemoryStores(final TestInfo testInfo) throws InterruptedException {
        shouldScaleOutWithWarmupTasks(storeName -> Materialized.as(Stores.inMemoryKeyValueStore(storeName)), testInfo);
    }

    @Test
    public void shouldScaleOutWithWarmupTasksAndPersistentStores(final TestInfo testInfo) throws InterruptedException {
        shouldScaleOutWithWarmupTasks(storeName -> Materialized.as(Stores.persistentKeyValueStore(storeName)), testInfo);
    }

    private void shouldScaleOutWithWarmupTasks(final Function<String, Materialized<Object, Object, KeyValueStore<Bytes, byte[]>>> materializedFunction,
                                               final TestInfo testInfo) throws InterruptedException {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId_" + System.currentTimeMillis() + "_" + testId;
        final String inputTopic = "input" + testId;
        final Set<TopicPartition> inputTopicPartitions = Set.of(
            new TopicPartition(inputTopic, 0),
            new TopicPartition(inputTopic, 1)
        );

        final String storeName = "store" + testId;
        final String storeChangelog = appId + "-store" + testId + "-changelog";
        final Set<TopicPartition> changelogTopicPartitions = Set.of(
            new TopicPartition(storeChangelog, 0),
            new TopicPartition(storeChangelog, 1)
        );

        CLUSTER.deleteAllTopics();
        CLUSTER.createTopic(inputTopic, 2, 2);
        CLUSTER.createTopic(storeChangelog, 2, 2, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, materializedFunction.apply(storeName));
        final Topology topology = builder.build();

        final int numberOfRecords = 500;

        produceTestData(inputTopic, numberOfRecords);

        try (final KafkaStreams kafkaStreams0 = new KafkaStreams(topology, streamsProperties(appId));
             final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, streamsProperties(appId));
             final Consumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties())) {
            kafkaStreams0.start();

            // sanity check: just make sure we actually wrote all the input records
            waitForTopicSize(inputTopicPartitions, consumer, numberOfRecords, "input topic");

            // wait until all the input records are in the changelog
            waitForTopicSize(changelogTopicPartitions, consumer, numberOfRecords, "changelog");

            final AtomicLong instance1TotalRestored = new AtomicLong(-1);
            final AtomicLong instance1NumRestored = new AtomicLong(-1);
            final CountDownLatch restoreCompleteLatch = new CountDownLatch(1);
            kafkaStreams1.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(final TopicPartition topicPartition,
                                           final String storeName,
                                           final long startingOffset,
                                           final long endingOffset) {
                }

                @Override
                public void onBatchRestored(final TopicPartition topicPartition,
                                            final String storeName,
                                            final long batchEndOffset,
                                            final long numRestored) {
                    // this test's topology/scale-out shape guarantees exactly one task ever warms up on
                    // kafkaStreams1, so a plain set is sufficient (see findStandbyTaskId's own "at most one" check).
                    instance1NumRestored.set(numRestored);
                }

                @Override
                public void onRestoreEnd(final TopicPartition topicPartition,
                                         final String storeName,
                                         final long totalRestored) {
                    instance1TotalRestored.set(totalRestored);
                    restoreCompleteLatch.countDown();
                }
            });

            kafkaStreams1.start();

            // the new instance has no caught-up copy of any stateful task, so the target assignment's mismatch
            // should be staged behind a warm-up task rather than promoted to active right away.
            final AtomicReference<TaskId> warmupTaskId = new AtomicReference<>();
            TestUtils.waitForCondition(
                () -> {
                    final TaskId standbyTaskId = findStandbyTaskId(kafkaStreams1);
                    if (standbyTaskId == null) {
                        return false;
                    }
                    warmupTaskId.set(standbyTaskId);
                    return true;
                },
                120_000L,
                () -> "Never saw a warm-up task assigned to the new instance after scale out."
            );

            // once the warm-up catches up (acceptable.recovery.lag=0, i.e. fully caught up), it should be promoted
            // to active.
            TestUtils.waitForCondition(
                () -> isActiveTask(kafkaStreams1, warmupTaskId.get()),
                120_000L,
                () -> "The warm-up task " + warmupTaskId.get() + " was never promoted to active on the new instance. " +
                    "Note, if this does fail, check and see if the new instance just failed to catch up within" +
                    " the refinement interval. A full minute should be long enough to read ~500 records" +
                    " in any test environment, but you never know..."
            );

            // the promoted task should be exclusively owned by kafkaStreams1 now; with no standbys configured
            // (streams.num.standby.replicas defaults to 0), kafkaStreams0 should have no trace of the task left
            // in any role at all. Poll instead of checking once: a thread's exposed metadata snapshot is only
            // refreshed on transition back into RUNNING, so it can briefly still show the pre-revocation state.
            TestUtils.waitForCondition(
                () -> !hasTask(kafkaStreams0, warmupTaskId.get()),
                120_000L,
                () -> "kafkaStreams0 should have released " + warmupTaskId.get() + " once it was promoted to active on kafkaStreams1"
            );

            restoreCompleteLatch.await();
            // We should finalize the restoration without having restored any records (because they're already in
            // the store). Otherwise, we failed to properly re-use the state from the warm-up task.
            assertEquals(0L, instance1TotalRestored.get());
            // Belt-and-suspenders check that we never even attempt to restore any records.
            assertEquals(-1L, instance1NumRestored.get());
        }
    }

    private static TaskId findStandbyTaskId(final KafkaStreams streams) {
        final List<TaskId> standbyTaskIds = new ArrayList<>();
        for (final ThreadMetadata threadMetadata : streams.metadataForLocalThreads()) {
            for (final TaskMetadata taskMetadata : threadMetadata.standbyTasks()) {
                standbyTaskIds.add(taskMetadata.taskId());
            }
        }
        // this test's topology/scale-out shape (2 tasks, 1 process gaining a member) means only the one task
        // whose target owner actually changed should ever be staged behind a warm-up.
        assertTrue(standbyTaskIds.size() <= 1, "Expected at most one warm-up task on the new instance, got: " + standbyTaskIds);
        return standbyTaskIds.isEmpty() ? null : standbyTaskIds.get(0);
    }

    private static boolean isActiveTask(final KafkaStreams streams, final TaskId taskId) {
        for (final ThreadMetadata threadMetadata : streams.metadataForLocalThreads()) {
            for (final TaskMetadata taskMetadata : threadMetadata.activeTasks()) {
                if (taskMetadata.taskId().equals(taskId)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasTask(final KafkaStreams streams, final TaskId taskId) {
        for (final ThreadMetadata threadMetadata : streams.metadataForLocalThreads()) {
            for (final TaskMetadata taskMetadata : threadMetadata.activeTasks()) {
                if (taskMetadata.taskId().equals(taskId)) {
                    return true;
                }
            }
            for (final TaskMetadata taskMetadata : threadMetadata.standbyTasks()) {
                if (taskMetadata.taskId().equals(taskId)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void produceTestData(final String inputTopic, final int numberOfRecords) {
        final String kilo = getKiloByteValue();

        final Properties producerProperties = mkProperties(
            mkMap(
                mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ProducerConfig.ACKS_CONFIG, "all"),
                mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            )
        );

        final List<KeyValueTimestamp<String, String>> records = new ArrayList<>(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(new KeyValueTimestamp<>(String.valueOf(i), kilo, System.currentTimeMillis()));
        }
        IntegrationTestUtils.produceSynchronously(producerProperties, false, inputTopic, Optional.empty(), records);
    }

    private static Properties getConsumerProperties() {
        return mkProperties(
                mkMap(
                    mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                    mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                    mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                )
            );
    }

    private static String getKiloByteValue() {
        return "0".repeat(1000);
    }

    private static Properties streamsProperties(final String appId) {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault())),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName())
            )
        );
    }

    private static long getEndOffsetSum(final Set<TopicPartition> changelogTopicPartitions,
                                        final Consumer<String, String> consumer) {
        long sum = 0;
        final Collection<Long> values = consumer.endOffsets(changelogTopicPartitions).values();
        for (final Long value : values) {
            sum += value;
        }
        return sum;
    }

    private static void waitForTopicSize(final Set<TopicPartition> partitions,
                                         final Consumer<String, String> consumer,
                                         final int expectedRecords,
                                         final String topicDescription) throws InterruptedException {
        final AtomicLong lastSeenSize = new AtomicLong();
        TestUtils.waitForCondition(
            () -> {
                final long size = getEndOffsetSum(partitions, consumer);
                lastSeenSize.set(size);
                return size == expectedRecords;
            },
            120_000L,
            () -> "Input records haven't all been written to the " + topicDescription + ": " + lastSeenSize.get()
        );
    }
}
