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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that verifies the deferred future tracking fix for the race condition
 * between the StateUpdater thread and the StreamThread when a rebalance triggers task
 * removal while the StateUpdater is blocked during changelog restoration.
 *
 * <p>Without the fix, the race condition chain is:
 * <ol>
 *   <li>StateUpdater thread is blocked in restoration (e.g., RocksDB write stall)</li>
 *   <li>A rebalance occurs, StreamThread calls {@code waitForFuture()} which times out</li>
 *   <li>The task is silently dropped — nobody tracks it</li>
 *   <li>StateUpdater eventually processes the REMOVE, suspends the task (stores NOT closed)</li>
 *   <li>The orphaned task holds the RocksDB file LOCK</li>
 *   <li>On restart, {@code RocksDB.open()} fails with {@code ProcessorStateException}</li>
 * </ol>
 *
 * <p>With the fix ({@code pendingRemoveFutures} tracking in {@code TaskManager}):
 * <ol>
 *   <li>When {@code waitForFuture()} times out, the future is stashed (not discarded)</li>
 *   <li>On the next {@code checkStateUpdater()} call, completed futures are polled</li>
 *   <li>The returned task is closed dirty, releasing the RocksDB LOCK</li>
 *   <li>On restart, {@code RocksDB.open()} succeeds — no LOCK conflict</li>
 * </ol>
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1035">KIP-1035</a>
 */
@Timeout(120)
public class StateUpdaterRestorationRaceIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "input-topic";
    private static final String BLOCKING_STORE_NAME = "blocking-store";
    private static final String ROCKSDB_STORE_NAME = "rocksdb-store";
    private static final int NUM_PARTITIONS = 6;

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private String appId;
    private KafkaStreams streams1;
    private KafkaStreams streams2;

    // Controls whether the restore callback should block
    private final AtomicBoolean blockDuringRestore = new AtomicBoolean(false);
    // Ensures only the first restore record triggers the block
    private final AtomicBoolean hasBlocked = new AtomicBoolean(false);
    // Signaled when restoration has started (StateUpdater is in restore)
    private final CountDownLatch restorationStartedLatch = new CountDownLatch(1);
    // Released to unblock the StateUpdater's restore callback
    private final CountDownLatch restoreBlockLatch = new CountDownLatch(1);

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException, IOException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, NUM_PARTITIONS, 1);
        appId = "app-" + safeUniqueTestName(testInfo);
    }

    @AfterEach
    public void after() {
        // Release the block latch in case the test failed before doing so
        restoreBlockLatch.countDown();
        if (streams1 != null) {
            streams1.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }
        cluster.stop();
    }

    /**
     * Verifies that when {@code waitForFuture()} times out during a rebalance, the deferred
     * future tracking in {@code TaskManager} cleans up the leaked task and releases the RocksDB
     * LOCK, allowing a subsequent restart to succeed without {@code ProcessorStateException}.
     *
     * <p>Uses a two-store topology:
     * <ul>
     *   <li>{@code blocking-store} (MockKeyValueStore): blocks the StateUpdater thread during restoration</li>
     *   <li>{@code rocksdb-store} (real RocksDB): acquires the file LOCK that would cause a conflict
     *       if the task were leaked</li>
     * </ul>
     *
     * <p>Test flow:
     * <ol>
     *   <li>Start instance 1 — both stores initialized, RocksDB LOCK acquired, restoration blocks</li>
     *   <li>Start instance 2 → rebalance → {@code waitForFuture()} times out → future stashed</li>
     *   <li>Unblock restoration → StateUpdater processes REMOVE → future completes</li>
     *   <li>Next {@code checkStateUpdater()} call → {@code processPendingRemoveFutures()} →
     *       task closed dirty → RocksDB LOCK released</li>
     *   <li>Close both instances, restart instance 1 with same state directory</li>
     *   <li>Assert: instance starts successfully (no ProcessorStateException)</li>
     * </ol>
     *
     * <p>Without the fix, step 6 would fail with {@code ProcessorStateException: Error opening store}
     * because the orphaned task's RocksDB handle would still hold the file LOCK.
     */
    @Test
    public void shouldCleanUpLeakedTaskAndReleaseRocksDBLockAfterWaitForFutureTimeout() throws Exception {
        blockDuringRestore.set(true);

        // Pre-create changelog topics for both stores
        final String blockingChangelog = appId + "-" + BLOCKING_STORE_NAME + "-changelog";
        final String rocksdbChangelog = appId + "-" + ROCKSDB_STORE_NAME + "-changelog";
        cluster.createTopic(blockingChangelog, NUM_PARTITIONS, 1);
        cluster.createTopic(rocksdbChangelog, NUM_PARTITIONS, 1);
        populateChangelog(blockingChangelog, 50);
        populateChangelog(rocksdbChangelog, 50);

        final String stateDir1 = TestUtils.tempDirectory().getPath();
        final Properties props1 = props(stateDir1);
        // waitForFuture timeout = maxPollIntervalMs / 2 = 7.5s
        props1.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 15_000);

        try (final LogCaptureAppender taskManagerAppender = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams1 = new KafkaStreams(buildTopologyWithRocksDB(), props1);
            streams1.start();

            // Wait for the StateUpdater to begin restoration (and block on the latch)
            assertTrue(
                restorationStartedLatch.await(30, TimeUnit.SECONDS),
                "Restoration never started on instance 1"
            );

            // Start instance 2 to trigger a rebalance while restoration is blocked.
            // StreamThread will call handleTasksInStateUpdater() → waitForFuture() → timeout
            // → future stashed in pendingRemoveFutures
            final String stateDir2 = TestUtils.tempDirectory().getPath();
            streams2 = new KafkaStreams(buildTopologyWithRocksDB(), props(stateDir2));
            streams2.start();

            // Wait for waitForFuture timeout — proves the deferred tracking path was exercised.
            TestUtils.waitForCondition(
                () -> taskManagerAppender.getMessages().stream()
                    .anyMatch(msg -> msg.contains("Deferring cleanup to next checkStateUpdater()")),
                30_000,
                "Expected waitForFuture() to time out and defer cleanup"
            );

            // Unblock restoration so the StateUpdater can process the pending REMOVE action.
            // With the fix: the completed future is picked up by processPendingRemoveFutures()
            // on the next checkStateUpdater() call, the task is closed dirty, and the RocksDB
            // LOCK is released.
            restoreBlockLatch.countDown();

            // Wait for processPendingRemoveFutures() to clean up the leaked task:
            // 1. StateUpdater finishes restoration and processes the REMOVE
            // 2. checkStateUpdater() calls processPendingRemoveFutures()
            // 3. closeTaskDirty() closes the RocksDB handle and releases the LOCK
            TestUtils.waitForCondition(
                () -> taskManagerAppender.getMessages().stream()
                    .anyMatch(msg -> msg.contains("Processing deferred removal of task")),
                30_000,
                "Expected processPendingRemoveFutures() to clean up the leaked task"
            );

            // Close both instances
            streams2.close(Duration.ofSeconds(10));
            streams2 = null;
            streams1.close(Duration.ofSeconds(10));
            streams1 = null;

            // Restart instance 1 with the SAME state directory.
            // Without the fix: the orphaned task's RocksDB handle still holds the file LOCK
            //   → RocksDB.open() fails → ProcessorStateException
            // With the fix: the task was closed dirty, LOCK released
            //   → RocksDB.open() succeeds → instance starts normally
            blockDuringRestore.set(false);
            streams1 = new KafkaStreams(buildTopologyWithRocksDB(), props1);
            streams1.start();

            // Assert: instance starts successfully and reaches RUNNING.
            // Without the fix, this would fail with ProcessorStateException from the
            // RocksDB LOCK conflict. With the fix, processPendingRemoveFutures() closed
            // the leaked task dirty, releasing the LOCK before restart.
            TestUtils.waitForCondition(
                () -> streams1.state() == KafkaStreams.State.RUNNING,
                30_000,
                "Instance should reach RUNNING state — the deferred future tracking " +
                "should have cleaned up the leaked task and released the RocksDB LOCK"
            );
        }
    }

    /**
     * Builds a topology with two stores: a blocking MockKeyValueStore (to halt the StateUpdater
     * during restoration) and a real RocksDB store (whose file LOCK causes ProcessorStateException
     * when the task is leaked and later reassigned).
     */
    private TopologyWrapper buildTopologyWithRocksDB() {
        // Store 1: MockKeyValueStore with blocking restore callback
        final StoreBuilder<KeyValueStore<Object, Object>> blockingStoreBuilder =
            new AbstractStoreBuilder<>(BLOCKING_STORE_NAME, Serdes.Integer(), Serdes.String(), new MockTime()) {
                @Override
                public KeyValueStore<Object, Object> build() {
                    return new MockKeyValueStore(name, true) {
                        @Override
                        public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                            stateStoreContext.register(root, (key, value) -> {
                                if (blockDuringRestore.get() && !hasBlocked.getAndSet(true)) {
                                    restorationStartedLatch.countDown();
                                    try {
                                        restoreBlockLatch.await(60, TimeUnit.SECONDS);
                                    } catch (final InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            });
                            initialized = true;
                            closed = false;
                        }
                    };
                }
            };

        // Store 2: Real RocksDB store — acquires file LOCK during init
        final StoreBuilder<KeyValueStore<Integer, String>> rocksdbStoreBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ROCKSDB_STORE_NAME),
                Serdes.Integer(), Serdes.String());

        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("source", INPUT_TOPIC);
        topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        topology.addStateStore(blockingStoreBuilder, "processor");
        topology.addStateStore(rocksdbStoreBuilder, "processor");
        return topology;
    }

    private Properties props(final String stateDir) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 10_000);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 3_000);
        return props;
    }

    /**
     * Produce records directly to the changelog topic so that restoration is needed
     * when an instance starts with an empty state directory.
     */
    private void populateChangelog(final String changelogTopic, final int recordsPerPartition) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (final KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerConfig)) {
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                for (int i = 0; i < recordsPerPartition; i++) {
                    producer.send(new ProducerRecord<>(changelogTopic, partition, i, "value-" + i));
                }
            }
            producer.flush();
        }
    }
}
