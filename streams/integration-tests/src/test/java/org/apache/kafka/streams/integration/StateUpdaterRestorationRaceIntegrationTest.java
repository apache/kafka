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
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
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
import org.apache.kafka.streams.errors.StreamsException;
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
 * Integration test that reproduces the race condition between the StateUpdater thread
 * and the StreamThread when a rebalance triggers task removal while the StateUpdater
 * is blocked during changelog restoration.
 *
 * <p>The race condition chain:
 * <ol>
 *   <li>StateUpdater thread is blocked in {@code restoreBatch()} (simulated via CountDownLatch)</li>
 *   <li>A rebalance occurs (triggered by a second instance joining)</li>
 *   <li>StreamThread calls {@code handleTasksInStateUpdater()} which enqueues a REMOVE action</li>
 *   <li>StreamThread blocks in {@code waitForFuture()} because StateUpdater can't process the REMOVE</li>
 *   <li>{@code max.poll.interval.ms} is exceeded because StreamThread can't poll</li>
 *   <li>Consumer is kicked from group, triggering another rebalance cascade</li>
 * </ol>
 *
 * <p>In production, when the StateUpdater is blocked long enough (e.g., RocksDB write stall),
 * the {@code waitForFuture()} 5-minute timeout is reached, the task is leaked, and on the next
 * rebalance the leaked task still holds the RocksDB LOCK, causing a {@code ProcessorStateException}.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1035">KIP-1035</a>
 */
@Timeout(120)
public class StateUpdaterRestorationRaceIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "input-topic";
    private static final String STORE_NAME = "test-store";
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
        // Pre-create the changelog topic with data so restoration is needed on first start
        final String changelogTopic = appId + "-" + STORE_NAME + "-changelog";
        cluster.createTopic(changelogTopic, NUM_PARTITIONS, 1);
        populateChangelog(changelogTopic, 50);
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
     * Demonstrates that when the StateUpdater is blocked during changelog restoration
     * and a rebalance requires task removal, the StreamThread blocks in waitForFuture()
     * and cannot poll, leading to max.poll.interval.ms violation.
     *
     * <p>The changelog topic is pre-populated so that restoration is needed on startup.
     * The store's restore callback blocks via a CountDownLatch to simulate a RocksDB write stall.
     * While the StateUpdater is stuck in restoration, a second instance joins the group,
     * triggering a rebalance that requires tasks to be removed from the StateUpdater.
     *
     * <p>The test asserts that the consumer poll timeout fires, which proves the StreamThread
     * was blocked in waitForFuture() and unable to poll. In production, with longer stalls,
     * this leads to task leaks and RocksDB LOCK conflicts.
     */
    @Test
    public void shouldBlockStreamThreadWhenStateUpdaterCannotProcessRemoveDuringRestore() throws Exception {
        blockDuringRestore.set(true);

        final String stateDir1 = TestUtils.tempDirectory().getPath();
        final Properties props1 = props(stateDir1);
        // Short poll interval so the timeout fires quickly when StreamThread is blocked in waitForFuture()
        props1.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 15_000);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(ConsumerCoordinator.class)) {
            streams1 = new KafkaStreams(buildTopology(), props1);
            streams1.start();

            // Wait for the StateUpdater to begin restoration (and block on the latch)
            assertTrue(
                restorationStartedLatch.await(30, TimeUnit.SECONDS),
                "Restoration never started on instance 1"
            );

            // Start a second instance to trigger a rebalance while restoration is blocked.
            // This rebalance will reassign some partitions to instance 2,
            // forcing instance 1's StreamThread to call handleTasksInStateUpdater()
            // which will try to REMOVE tasks from the StateUpdater via waitForFuture().
            final String stateDir2 = TestUtils.tempDirectory().getPath();
            streams2 = new KafkaStreams(buildTopology(), props(stateDir2));
            streams2.start();

            // Wait for the poll timeout to fire.
            // The cascade:
            // 1. Rebalance assigns some of instance 1's tasks to instance 2
            // 2. Instance 1's StreamThread calls stateUpdater.remove() for those tasks
            // 3. StreamThread blocks in waitForFuture() — StateUpdater can't process REMOVE
            //    because it's stuck in the restore callback
            // 4. After 15s (max.poll.interval.ms), the consumer is kicked from the group
            TestUtils.waitForCondition(
                () -> appender.getMessages().stream()
                    .anyMatch(msg -> msg.contains("consumer poll timeout has expired")),
                60_000,
                "Expected consumer poll timeout to fire due to StreamThread being blocked " +
                "in waitForFuture() while StateUpdater is stuck in restoration"
            );

            // Release the restore block so cleanup can proceed cleanly
            restoreBlockLatch.countDown();
        }
    }

    /**
     * Demonstrates the full crash chain: when the StateUpdater is blocked during restoration
     * and {@code waitForFuture()} times out (bounded by Change 4 to {@code maxPollIntervalMs / 2}),
     * the task is leaked with RocksDB still open. When the partition is reassigned, the new task
     * tries to open the same RocksDB directory but the leaked task still holds the LOCK file,
     * resulting in a {@code ProcessorStateException}.
     *
     * <p>Uses a two-store topology:
     * <ul>
     *   <li>{@code blocking-store} (MockKeyValueStore): blocks the StateUpdater thread during restoration</li>
     *   <li>{@code rocksdb-store} (real RocksDB): acquires the file LOCK that causes the conflict</li>
     * </ul>
     *
     * <p>The crash chain:
     * <ol>
     *   <li>Both stores are initialized — RocksDB opens and acquires file LOCK</li>
     *   <li>StateUpdater blocks on blocking-store's restore callback</li>
     *   <li>Instance 2 joins → rebalance → StreamThread calls waitForFuture() → timeout → task leaked</li>
     *   <li>Restoration unblocked → StateUpdater processes REMOVE → task.suspend() (stores NOT closed)</li>
     *   <li>Instance 2 closed → rebalance → partition reassigned to instance 1</li>
     *   <li>New task: stateDirectory.lock(id) passes (same thread, re-entrant)</li>
     *   <li>RocksDB.open() → file LOCK held by orphaned task → ProcessorStateException</li>
     * </ol>
     */
    @Test
    public void shouldThrowProcessorStateExceptionWhenLeakedTaskHoldsRocksDBLock() throws Exception {
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
        // With Change 4, waitForFuture timeout = maxPollIntervalMs / 2 = 7.5s
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
            final String stateDir2 = TestUtils.tempDirectory().getPath();
            streams2 = new KafkaStreams(buildTopologyWithRocksDB(), props(stateDir2));
            streams2.start();

            // Wait for waitForFuture timeout — proves the task was leaked.
            // The leaked task has RocksDB open with the file LOCK held.
            TestUtils.waitForCondition(
                () -> taskManagerAppender.getMessages().stream()
                    .anyMatch(msg -> msg.contains("wasn't able to remove task")),
                30_000,
                "Expected waitForFuture() to time out, leaking the task"
            );

            // Unblock restoration so the StateUpdater can process the pending REMOVE action.
            // The REMOVE calls task.suspend() which transitions to SUSPENDED but does NOT
            // close state stores — RocksDB remains open with the file LOCK held.
            // The completed future result (containing the suspended task) is never retrieved
            // because waitForFuture() already returned null — the task is orphaned.
            restoreBlockLatch.countDown();

            // Give the StateUpdater time to finish restoration and process the REMOVE
            Thread.sleep(3000);

            // Close both instances. The orphaned task is NOT tracked by either the
            // StreamThread (waitForFuture returned null) or the StateUpdater (REMOVE was
            // processed). So close() does NOT close the orphaned task's RocksDB — the
            // native RocksDB handle keeps the file LOCK in this JVM process.
            streams2.close(Duration.ofSeconds(10));
            streams2 = null;
            streams1.close(Duration.ofSeconds(10));
            streams1 = null;

            // Restart a new instance with the SAME state directory.
            // When the new instance creates tasks and initializes state stores:
            //   1. RocksDBStore.init() → openRocksDB() → RocksDB.open()
            //   2. The orphaned task's RocksDB handle (still alive in this JVM) holds the
            //      file LOCK on the same directory
            //   3. RocksDB.open() fails with "lock hold by current process" / "No locks available"
            //   4. Wrapped as ProcessorStateException("Error opening store...")
            // This reproduces the exact production crash: after a waitForFuture timeout
            // leaks a task, the next attempt to use the same state directory fails.
            streams1 = new KafkaStreams(buildTopologyWithRocksDB(), props1);

            // The ProcessorStateException may be thrown during start() (synchronous init)
            // or transition the instance to ERROR state (async init). Handle both paths.
            try {
                streams1.start();

                TestUtils.waitForCondition(
                    () -> streams1.state() == KafkaStreams.State.ERROR,
                    30_000,
                    "Instance should enter ERROR state due to ProcessorStateException " +
                    "from RocksDB LOCK conflict with the orphaned task's still-open handle"
                );
            } catch (final StreamsException e) {
                // ProcessorStateException extends StreamsException — verify the root cause
                // is a RocksDB LOCK conflict from the orphaned task
                assertTrue(
                    findRocksDBLockException(e),
                    "Expected RocksDB LOCK conflict but got: " + e.getMessage()
                );
            }
        }
    }

    private TopologyWrapper buildTopology() {
        final StoreBuilder<KeyValueStore<Object, Object>> storeBuilder =
            new AbstractStoreBuilder<>(STORE_NAME, Serdes.Integer(), Serdes.String(), new MockTime()) {
                @Override
                public KeyValueStore<Object, Object> build() {
                    return new MockKeyValueStore(name, true) {
                        @Override
                        public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                            // Register a restore callback that can block to simulate a long-running restoration
                            stateStoreContext.register(root, (key, value) -> {
                                if (blockDuringRestore.get() && !hasBlocked.getAndSet(true)) {
                                    // Signal that restoration has started
                                    restorationStartedLatch.countDown();
                                    try {
                                        // Block the StateUpdater thread to simulate a RocksDB write stall.
                                        // 30s is long enough for max.poll.interval.ms (15s) to expire
                                        // and trigger the poll timeout that proves the race condition.
                                        // Only the first record blocks — subsequent records pass through.
                                        restoreBlockLatch.await(30, TimeUnit.SECONDS);
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

        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("source", INPUT_TOPIC);
        topology.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        topology.addStateStore(storeBuilder, "processor");
        return topology;
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

    /**
     * Traverses the exception cause chain looking for a RocksDB LOCK conflict message.
     * The exact message from RocksDB is: "lock hold by current process... LOCK: No locks available"
     */
    private boolean findRocksDBLockException(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            final String message = current.getMessage();
            if (message != null && (message.contains("LOCK") || message.contains("No locks available"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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