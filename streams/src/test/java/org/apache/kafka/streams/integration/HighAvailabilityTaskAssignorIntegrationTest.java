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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Category(IntegrationTest.class)
public class HighAvailabilityTaskAssignorIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Rule
    public TestName testName = new TestName();

    @Test
    public void shouldScaleOutWithWarmupTasksAndInMemoryStores() throws InterruptedException {
        // NB: this test takes at least a minute to run, because it needs a probing rebalance, and the minimum
        // value is one minute
        shouldScaleOutWithWarmupTasks(storeName -> Materialized.as(Stores.inMemoryKeyValueStore(storeName)));
    }

    @Test
    public void shouldScaleOutWithWarmupTasksAndPersistentStores() throws InterruptedException {
        // NB: this test takes at least a minute to run, because it needs a probing rebalance, and the minimum
        // value is one minute
        shouldScaleOutWithWarmupTasks(storeName -> Materialized.as(Stores.persistentKeyValueStore(storeName)));
    }

    private void shouldScaleOutWithWarmupTasks(final Function<String, Materialized<Object, Object, KeyValueStore<Bytes, byte[]>>> materializedFunction) throws InterruptedException {
        final String testId = safeUniqueTestName(getClass(), testName);
        final String appId = "appId_" + System.currentTimeMillis() + "_" + testId;
        final String inputTopic = "input" + testId;
        final Set<TopicPartition> inputTopicPartitions = mkSet(
            new TopicPartition(inputTopic, 0),
            new TopicPartition(inputTopic, 1)
        );

        final String storeName = "store" + testId;
        final String storeChangelog = appId + "-store" + testId + "-changelog";
        final Set<TopicPartition> changelogTopicPartitions = mkSet(
            new TopicPartition(storeChangelog, 0),
            new TopicPartition(storeChangelog, 1)
        );

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, 2, inputTopic, storeChangelog);

        final ReentrantLock assignmentLock = new ReentrantLock();
        final AtomicInteger assignmentsCompleted = new AtomicInteger(0);
        final Map<Integer, Boolean> assignmentsStable = new ConcurrentHashMap<>();
        final AtomicBoolean assignmentStable = new AtomicBoolean(false);
        final AssignmentListener assignmentListener =
            stable -> {
                assignmentLock.lock();
                try {
                    final int thisAssignmentIndex = assignmentsCompleted.incrementAndGet();
                    assignmentsStable.put(thisAssignmentIndex, stable);
                    assignmentStable.set(stable);
                } finally {
                    assignmentLock.unlock();
                }
            };

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, materializedFunction.apply(storeName));
        final Topology topology = builder.build();

        final int numberOfRecords = 500;

        produceTestData(inputTopic, numberOfRecords);

        try (final KafkaStreams kafkaStreams0 = new KafkaStreams(topology, streamsProperties(appId, assignmentListener));
             final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, streamsProperties(appId, assignmentListener));
             final Consumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties())) {
            kafkaStreams0.start();

            // sanity check: just make sure we actually wrote all the input records
            TestUtils.waitForCondition(
                () -> getEndOffsetSum(inputTopicPartitions, consumer) == numberOfRecords,
                120_000L,
                () -> "Input records haven't all been written to the input topic: " + getEndOffsetSum(inputTopicPartitions, consumer)
            );

            // wait until all the input records are in the changelog
            TestUtils.waitForCondition(
                () -> getEndOffsetSum(changelogTopicPartitions, consumer) == numberOfRecords,
                120_000L,
                () -> "Input records haven't all been written to the changelog: " + getEndOffsetSum(changelogTopicPartitions, consumer)
            );

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
                    instance1NumRestored.accumulateAndGet(
                        numRestored,
                        (prev, restored) -> prev == -1 ? restored : prev + restored
                    );
                }

                @Override
                public void onRestoreEnd(final TopicPartition topicPartition,
                                         final String storeName,
                                         final long totalRestored) {
                    instance1TotalRestored.accumulateAndGet(
                        totalRestored,
                        (prev, restored) -> prev == -1 ? restored : prev + restored
                    );
                    restoreCompleteLatch.countDown();
                }
            });
            final int assignmentsBeforeScaleOut = assignmentsCompleted.get();
            kafkaStreams1.start();
            TestUtils.waitForCondition(
                () -> {
                    assignmentLock.lock();
                    try {
                        if (assignmentsCompleted.get() > assignmentsBeforeScaleOut) {
                            assertFalseNoRetry(
                                assignmentsStable.get(assignmentsBeforeScaleOut + 1),
                                "the first assignment after adding a node should be unstable while we warm up the state."
                            );
                            return true;
                        } else {
                            return false;
                        }
                    } finally {
                        assignmentLock.unlock();
                    }
                },
                120_000L,
                "Never saw a first assignment after scale out: " + assignmentsCompleted.get()
            );

            TestUtils.waitForCondition(
                assignmentStable::get,
                120_000L,
                "Assignment hasn't become stable: " + assignmentsCompleted.get() +
                    " Note, if this does fail, check and see if the new instance just failed to catch up within" +
                    " the probing rebalance interval. A full minute should be long enough to read ~500 records" +
                    " in any test environment, but you never know..."
            );

            restoreCompleteLatch.await();
            // We should finalize the restoration without having restored any records (because they're already in
            // the store. Otherwise, we failed to properly re-use the state from the standby.
            assertThat(instance1TotalRestored.get(), is(0L));
            // Belt-and-suspenders check that we never even attempt to restore any records.
            assertThat(instance1NumRestored.get(), is(-1L));
        }
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

        try (final Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (int i = 0; i < numberOfRecords; i++) {
                producer.send(new ProducerRecord<>(inputTopic, String.valueOf(i), kilo));
            }
        }
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
        final StringBuilder kiloBuilder = new StringBuilder(1000);
        for (int i = 0; i < 1000; i++) {
            kiloBuilder.append('0');
        }
        return kiloBuilder.toString();
    }

    private static void assertFalseNoRetry(final boolean assertion, final String message) {
        if (assertion) {
            throw new NoRetryException(
                new AssertionError(
                    message
                )
            );
        }
    }

    private static Properties streamsProperties(final String appId,
                                                final AssignmentListener configuredAssignmentListener) {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "0"),
                mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "0"), // make the warmup catch up completely
                mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "2"),
                mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "60000"),
                mkEntry(StreamsConfig.InternalConfig.ASSIGNMENT_LISTENER, configuredAssignmentListener),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100"),
                mkEntry(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, HighAvailabilityTaskAssignor.class.getName()),
                // Increasing the number of threads to ensure that a rebalance happens each time a consumer sends a rejoin (KAFKA-10455)
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 40)
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
}
