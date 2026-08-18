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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RebalanceTaskClosureIntegrationTest {

    private static final int NUM_BROKERS = 1;
    protected static final String INPUT_TOPIC_NAME = "input-topic";
    private static final int NUM_PARTITIONS = 3;

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private KafkaStreamsWrapper streams1;
    private KafkaStreamsWrapper streams2;
    private String safeTestName;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException, IOException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC_NAME, NUM_PARTITIONS, 1);
        safeTestName = safeUniqueTestName(testInfo);
    }

    @AfterEach
    public void after() {
        cluster.stop();
        if (streams1 != null) {
            streams1.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The conditions that we need to meet:
     * <p><ul>
     * <li>There is a task with an open store in {@link org.apache.kafka.streams.processor.internals.TasksRegistry#pendingTasksToInit}</li>
     * <li>StreamThread gets into PENDING_SHUTDOWN state, so that {@link StreamThread#isStartingRunningOrPartitionAssigned} returns false
     * before we call {@link StreamThread#checkStateUpdater} that would move the task to the StateUpdater </li>
     * </ul><p>
     * If all conditions are met, {@code TaskManager} needs to correctly close the open store during shutdown.
     * <p>
     * In order to have a task with an open store in the pending task list we first need to have an active task that gets converted
     * to a standby one during rebalance(see {@link org.apache.kafka.streams.processor.internals.TaskManager#closeAndRecycleTasks}).
     * Second, we need to avoid the second rebalance, to avoid that the pending tasks is closed during such a rebalance, ie, before we enter the shutdown phase.
     * <p>
     * For that this test:
     * <p><ul>
     * <li>starts a KS app and waits for it to fully start</li>
     * <li>starts another KS app which will trigger reassignment</li>
     * <li>waits for {@link CachedStateStore#clearCache} to be called(it's called during task recycle) and locks on it</li>
     * <li>sends a message with wrong types to crash the stream thread (this avoids a second rebalance, and enters shutdown directly)</li>
     * <li>shutdowns the first KS app</li>
     * <li>releases the lock</li>
     * </ul><p>
     * At this point {@link org.apache.kafka.streams.processor.internals.TaskManager#shutdown} will be called,
     * and we will have a pending task to init with an open store(because tasks keep their stores open during recycle).
     * <p>
     * This test verifies that the open store is closed during shutdown.
     */
    @Test
    public void shouldClosePendingTasksToInitAfterRebalance() throws Exception {
        final CountDownLatch recycleLatch = new CountDownLatch(1);
        final CountDownLatch pendingShutdownLatch = new CountDownLatch(1);
        // Count how many times we initialize and close stores
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = new AbstractStoreBuilder<>("testStateStore", Serdes.Integer(), Serdes.ByteArray(), new MockTime()) {

            @Override
            public KeyValueStore<Bytes, byte[]> build() {
                return new TestRocksDBStore(name, recycleLatch, pendingShutdownLatch, initCount, closeCount);
            }
        };

        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("ingest", INPUT_TOPIC_NAME);
        topology.addProcessor("my-processor", new MockApiProcessorSupplier<>(), "ingest");
        topology.addStateStore(storeBuilder, "my-processor");

        streams1 = new KafkaStreamsWrapper(topology, props("1"));
        streams1.setStreamThreadStateListener((t, newState, oldState) -> {
            if (newState == StreamThread.State.PENDING_SHUTDOWN) {
                pendingShutdownLatch.countDown();
            }
        });
        streams1.start();

        TestUtils.waitForCondition(() -> streams1.state() == KafkaStreams.State.RUNNING, "Streams never reached RUNNING state");

        streams2 = new KafkaStreamsWrapper(topology, props("2"));
        streams2.start();

        TestUtils.waitForCondition(() -> streams2.state() == KafkaStreams.State.RUNNING, "Streams never reached RUNNING state");

        // starting the second KS app triggered a rebalance. Which in turn will recycle active tasks that need to become standby.
        // That's exactly what we are waiting for
        recycleLatch.await();

        // sending a message with wrong key and value types to trigger a stream thread failure and avoid the second rebalance
        // note that writing this message does not trigger the crash right away -- the thread is still blocked inside `poll()` waiting for the shutdown latch to unlock to complete the previous, still ongoing rebalance
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC_NAME, List.of(new KeyValue<>("key", 1L)),
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, LongSerializer.class, new Properties()), cluster.time);
        // Now we can close both apps. The StreamThreadStateListener will unblock the clearCache call, letting the rebalance finish.
        // After the rebalance finished, the "poison pill" record gets picked up crashing the thread,
        // and starting the shutdown directly
        // We don't want to let the rebalance finish before we trigger the shutdown, because we want the stream thread to stop before it gets to moving pending tasks from task registry to state updater.
        streams1.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));
        streams2.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));

        assertEquals(initCount.get(), closeCount.get());
    }

    private Properties props(final String storePathSuffix) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + "/" + storePathSuffix);
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        return streamsConfiguration;
    }

    private static class TestRocksDBStore extends RocksDBStore implements CachedStateStore<Bytes, byte[]> {

        private final CountDownLatch recycleLatch;
        private final CountDownLatch pendingShutdownLatch;
        private final AtomicInteger initCount;
        private final AtomicInteger closeCount;

        public TestRocksDBStore(final String name,
                                final CountDownLatch recycleLatch,
                                final CountDownLatch pendingShutdownLatch,
                                final AtomicInteger initCount,
                                final AtomicInteger closeCount) {
            super(name, "rocksdb");
            this.recycleLatch = recycleLatch;
            this.pendingShutdownLatch = pendingShutdownLatch;
            this.initCount = initCount;
            this.closeCount = closeCount;
        }

        @Override
        public void init(final StateStoreContext stateStoreContext,
                         final StateStore root) {
            initCount.incrementAndGet();
            super.init(stateStoreContext, root);
        }

        @Override
        public boolean setFlushListener(final CacheFlushListener<Bytes, byte[]> listener,
                                        final boolean sendOldValues) {
            return false;
        }

        @Override
        public void flushCache() {
        }

        @Override
        public void clearCache() {
            // Clear cache is called during recycle, so we use it as a hook
            recycleLatch.countDown();
            try {
                // after we signaled via recycleLatch, that the task was converted into a "pending task",
                // we block the rebalance to complete, until we get the shutdown signal,
                // to avoid that the "pending task" get fully initialized
                // (otherwise, we don't have a pending task when the shutdown happens)
                pendingShutdownLatch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized void close() {
            closeCount.incrementAndGet();
            super.close();
        }
    }

}
