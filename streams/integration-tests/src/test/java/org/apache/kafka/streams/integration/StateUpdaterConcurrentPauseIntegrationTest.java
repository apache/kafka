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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateUpdater;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StateUpdaterConcurrentPauseIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String STORE_NAME = "counts-store";
    private static final int NUM_PARTITIONS = 2;
    private static final long TIMEOUT_MS = 60_000L;

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private KafkaStreamsWrapper streams1;
    private KafkaStreamsWrapper streams2;
    private String safeTestName;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException, IOException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC_NAME, NUM_PARTITIONS, 1);
        safeTestName = safeUniqueTestName(testInfo);
        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC_NAME,
            List.of(new KeyValue<>(0, "a"), new KeyValue<>(1, "b"), new KeyValue<>(2, "c"), new KeyValue<>(3, "d")),
            TestUtils.producerConfig(cluster.bootstrapServers(), IntegerSerializer.class, StringSerializer.class, new Properties()),
            cluster.time
        );
    }

    @AfterEach
    public void after() {
        if (streams1 != null) {
            streams1.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }
        cluster.stop();
    }

    @Test
    public void shouldNotListTheSameTaskTwiceWhilePausingAStandbyTask() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE_NAME));
        final Topology topology = builder.build();

        streams1 = new KafkaStreamsWrapper(topology, props("1"));
        streams2 = new KafkaStreamsWrapper(topology, props("2"));
        streams1.start();
        streams2.start();

        TestUtils.waitForCondition(() -> streams1.state() == KafkaStreams.State.RUNNING, TIMEOUT_MS, "streams1 not RUNNING");
        TestUtils.waitForCondition(() -> streams2.state() == KafkaStreams.State.RUNNING, TIMEOUT_MS, "streams2 not RUNNING");

        final StateUpdater stateUpdater = TestUtils.fieldValue(
                TestUtils.fieldValue(streams1.streamThreads().get(0), StreamThread.class, "taskManager"),
                TaskManager.class, "stateUpdater");

        final CountDownLatch removeInitiatedLatch = new CountDownLatch(1);
        final CountDownLatch removeCompletedLatch = new CountDownLatch(1);
        // Replacing the updatingTasks map in the state updater to be able to synchronize the calls so that they happen in the right order
        replaceUpdatingTasksMap(stateUpdater, removeInitiatedLatch, removeCompletedLatch);

        streams1.pause();
        assertTrue(removeInitiatedLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),
            "state updater thread never reached the pauseTask put->remove window");

        // Calling tasks() concurrently to avoid a deadlock
        final AtomicReference<List<TaskId>> tasksResult = new AtomicReference<>();
        final CountDownLatch readerDoneLatch = new CountDownLatch(1);
        final Thread reader = new Thread(() -> {
            try {
                tasksResult.set(stateUpdater.tasks().stream().map(Task::id).collect(Collectors.toList()));
            } finally {
                readerDoneLatch.countDown();
            }
        });
        reader.start();

        TestUtils.waitForCondition(
            () ->  readerDoneLatch.getCount() == 0 || reader.getState() == Thread.State.WAITING,
            TIMEOUT_MS,
            "tasks() reader never got blocked on the state updater lock"
        );

        removeCompletedLatch.countDown();
        reader.join();

        final List<TaskId> ids = tasksResult.get();
        assertEquals(ids.stream().distinct().count(), ids.size(),
            "DefaultStateUpdater.tasks() returned duplicate task ids while a pause was in flight: " + ids);
    }

    private void replaceUpdatingTasksMap(final StateUpdater stateUpdater,
                                         final CountDownLatch removeInitiatedLatch,
                                         final CountDownLatch removeCompletedLatch) throws Exception {
        final Object stateUpdaterThread = TestUtils.fieldValue(stateUpdater, stateUpdater.getClass(), "stateUpdaterThread");
        final ConcurrentHashMap<TaskId, Task> oldMap = TestUtils.fieldValue(stateUpdaterThread, stateUpdaterThread.getClass(), "updatingTasks");
        TestUtils.setFieldValue(stateUpdaterThread, "updatingTasks", new SynchronizedTestMap(oldMap, removeInitiatedLatch, removeCompletedLatch));
    }

    private Properties props(final String stateDirSuffix) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + "/" + stateDirSuffix);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1_000);
        return config;
    }

    private static class SynchronizedTestMap extends ConcurrentHashMap<TaskId, Task> {

        private final CountDownLatch removeInitiatedLatch;
        private final CountDownLatch removeCompletedLatch;
        private volatile boolean frozen = false;

        public SynchronizedTestMap(final ConcurrentHashMap<TaskId, Task> oldMap,
                                   final CountDownLatch removeInitiatedLatch,
                                   final CountDownLatch removeCompletedLatch) {
            super(oldMap);
            this.removeInitiatedLatch = removeInitiatedLatch;
            this.removeCompletedLatch = removeCompletedLatch;
        }

        @Override
        public Task remove(final Object key) {
            // Freeze the first removal (pauseTask's), holding the task in both maps until the test releases us.
            if (!frozen) {
                frozen = true;
                removeInitiatedLatch.countDown();
                try {
                    removeCompletedLatch.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.remove(key);
        }
    }
}
