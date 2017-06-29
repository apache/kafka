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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.util.Collections.EMPTY_SET;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamThreadTest {

    private final String clientId = "clientId";
    private final String applicationId = "stream-thread-test";
    private final MockTime mockTime = new MockTime();
    private final Metrics metrics = new Metrics();
    private final MockClientSupplier clientSupplier = new MockClientSupplier();
    private UUID processId = UUID.randomUUID();
    final KStreamBuilder builder = new KStreamBuilder();
    private final StreamsConfig config = new StreamsConfig(configProps(false));



    @Before
    public void setUp() throws Exception {
        processId = UUID.randomUUID();
    }

    private final TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private final TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private final TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private final TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private final TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private final TopicPartition t3p2 = new TopicPartition("topic3", 2);

    private final List<PartitionInfo> infos = Arrays.asList(
        new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 2, Node.noNode(), new Node[0], new Node[0])
    );

    private final Cluster metadata = new Cluster(
        "cluster",
        Collections.singleton(Node.noNode()),
        infos,
        Collections.<String>emptySet(),
        Collections.<String>emptySet());

    private final PartitionAssignor.Subscription subscription =
        new PartitionAssignor.Subscription(Arrays.asList("topic1", "topic2", "topic3"), subscriptionUserData());

    private ByteBuffer subscriptionUserData() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.allocate(4 + 16 + 4 + 4);
        // version
        buf.putInt(1);
        // encode client processId
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        // previously running tasks
        buf.putInt(0);
        // cached tasks
        buf.putInt(0);
        buf.rewind();
        return buf;
    }

    // task0 is unused
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(0, 3);
    private final TaskId task4 = new TaskId(1, 1);
    private final TaskId task5 = new TaskId(1, 2);

    private Properties configProps(final boolean enableEos) {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
                setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
                if (enableEos) {
                    setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                }
            }
        };
    }

    private static class TestStreamTask extends StreamTask {
        boolean committed = false;
        private boolean suspended;
        private boolean closed;
        private boolean closedStateManager;

        TestStreamTask(final TaskId id,
                       final String applicationId,
                       final Collection<TopicPartition> partitions,
                       final ProcessorTopology topology,
                       final Consumer<byte[], byte[]> consumer,
                       final Producer<byte[], byte[]> producer,
                       final Consumer<byte[], byte[]> restoreConsumer,
                       final StreamsConfig config,
                       final StreamsMetrics metrics,
                       final StateDirectory stateDirectory) {
            super(id,
                applicationId,
                partitions,
                topology,
                consumer,
                new StoreChangelogReader(restoreConsumer, Time.SYSTEM, 5000),
                config,
                metrics,
                stateDirectory,
                null,
                new MockTime(),
                producer);
        }

        @Override
        void commit(final boolean startNewTransaction) {
            super.commit(startNewTransaction);
            committed = true;
        }

        @Override
        protected void updateOffsetLimits() {}

        @Override
        public void resume() {
            if (!suspended || closed) {
                throw new IllegalStateException("Should not resume task that is not suspended or already closed.");
            }
            super.resume();
            suspended = false;
        }

        @Override
        void suspend(final boolean clean) {
            if (suspended || closed) {
                throw new IllegalStateException("Should not suspend task that is already suspended or closed.");
            }
            super.suspend(clean);
            suspended = true;
        }

        @Override
        public void close(final boolean clean) {
            if (closed && clean) {
                throw new IllegalStateException("Should not close task that is already closed.");
            }
            super.close(clean);
            closed = true;
        }

        @Override
        public void closeSuspended(final boolean clean, final RuntimeException firstException) {
            if (closed && clean) {
                throw new IllegalStateException("Should not close task that is not suspended or already closed.");
            }
            super.closeSuspended(clean, firstException);
            closed = true;
        }

        @Override
        void closeStateManager(final boolean writeCheckpoint) {
            super.closeStateManager(writeCheckpoint);
            closedStateManager = true;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChangeForSingleGroup() throws Exception {
        builder.addSource("source1", "topic1");

        final StreamThread thread = getStreamThread();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.RUNNING);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);
        Assert.assertEquals(stateListener.numChanges, 1);
        Assert.assertEquals(stateListener.oldState, StreamThread.State.RUNNING);
        Assert.assertEquals(stateListener.newState, StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = Collections.singletonList(t1p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(stateListener.numChanges, 3);
        Assert.assertEquals(stateListener.oldState, StreamThread.State.ASSIGNING_PARTITIONS);
        Assert.assertEquals(stateListener.newState, StreamThread.State.RUNNING);
        assertTrue(thread.tasks().containsKey(task1));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(1, thread.tasks().size());

        // revoke single partition
        revokedPartitions = assignedPartitions;
        activeTasks.clear();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertFalse(thread.tasks().containsKey(task1));
        assertEquals(0, thread.tasks().size());

        // assign different single partition
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));
        activeTasks.put(new TaskId(0, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(1, thread.tasks().size());

        // revoke different single partition and assign both partitions
        revokedPartitions = assignedPartitions;
        activeTasks.clear();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Arrays.asList(t1p1, t1p2);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        activeTasks.put(new TaskId(0, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all partitions and assign nothing
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().isEmpty());

        thread.close();
        assertTrue((thread.state() == StreamThread.State.PENDING_SHUTDOWN) ||
            (thread.state() == StreamThread.State.CREATED));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChangeForMultipleGroups() throws Exception {
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addSource("source3", "topic3");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source2", "source3");

        final StreamThread thread = getStreamThread();

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        final StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.RUNNING);

        final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        // revoke nothing
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);
        Assert.assertEquals(stateListener.numChanges, 1);
        Assert.assertEquals(stateListener.oldState, StreamThread.State.RUNNING);
        Assert.assertEquals(stateListener.newState, StreamThread.State.PARTITIONS_REVOKED);

        // assign four new partitions of second subtopology
        assignedPartitions = Arrays.asList(t2p1, t2p2, t3p1, t3p2);
        expectedGroup1 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p2, t3p2));
        activeTasks.put(new TaskId(1, 1), expectedGroup1);
        activeTasks.put(new TaskId(1, 2), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task4));
        assertTrue(thread.tasks().containsKey(task5));
        assertEquals(expectedGroup1, thread.tasks().get(task4).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task5).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke four partitions and assign three partitions of both subtopologies
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        activeTasks.put(new TaskId(0, 1), expectedGroup1);
        activeTasks.put(new TaskId(1, 1), expectedGroup2);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task4));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task4).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all three partitons and reassign the same three partitions (from different subtopologies)
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task4));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task4).partitions());
        assertEquals(2, thread.tasks().size());

        // revoke all partitions and assign nothing
        revokedPartitions = assignedPartitions;
        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assignedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().isEmpty());

        thread.close();
        assertTrue((thread.state() == StreamThread.State.PENDING_SHUTDOWN) ||
            (thread.state() == StreamThread.State.CREATED));
    }

    private final static String TOPIC = "topic";
    private final Set<TopicPartition> task0Assignment = Collections.singleton(new TopicPartition(TOPIC, 0));
    private final Set<TopicPartition> task1Assignment = Collections.singleton(new TopicPartition(TOPIC, 1));

    @SuppressWarnings("unchecked")
    @Test
    public void testHandingOverTaskFromOneToAnotherThread() throws Exception {
        builder.addStateStore(
            Stores
                .create("store")
                .withByteArrayKeys()
                .withByteArrayValues()
                .persistent()
                .build()
        );
        builder.addSource("source", TOPIC);

        clientSupplier.consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));

        final StreamThread thread1 = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId + 1,
            processId,
            metrics,
            Time.SYSTEM,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);
        final StreamThread thread2 = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId + 2,
            processId,
            metrics,
            Time.SYSTEM,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> task0 = Collections.singletonMap(new TaskId(0, 0), task0Assignment);
        final Map<TaskId, Set<TopicPartition>> task1 = Collections.singletonMap(new TaskId(0, 1), task1Assignment);

        final Map<TaskId, Set<TopicPartition>> thread1Assignment = new HashMap<>(task0);
        final Map<TaskId, Set<TopicPartition>> thread2Assignment = new HashMap<>(task1);

        thread1.setPartitionAssignor(new MockStreamsPartitionAssignor(thread1Assignment));
        thread2.setPartitionAssignor(new MockStreamsPartitionAssignor(thread2Assignment));

        // revoke (to get threads in correct state)
        thread1.rebalanceListener.onPartitionsRevoked(EMPTY_SET);
        thread2.rebalanceListener.onPartitionsRevoked(EMPTY_SET);

        // assign
        thread1.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread2.rebalanceListener.onPartitionsAssigned(task1Assignment);

        final Set<TaskId> originalTaskAssignmentThread1 = new HashSet<>();
        originalTaskAssignmentThread1.addAll(thread1.tasks().keySet());
        final Set<TaskId> originalTaskAssignmentThread2 = new HashSet<>();
        originalTaskAssignmentThread2.addAll(thread2.tasks().keySet());

        // revoke (task will be suspended)
        thread1.rebalanceListener.onPartitionsRevoked(task0Assignment);
        thread2.rebalanceListener.onPartitionsRevoked(task1Assignment);


        // assign reverted
        thread1Assignment.clear();
        thread1Assignment.putAll(task1);

        thread2Assignment.clear();
        thread2Assignment.putAll(task0);

        final Thread runIt = new Thread(new Runnable() {
            @Override
            public void run() {
                thread1.rebalanceListener.onPartitionsAssigned(task1Assignment);
            }
        });
        runIt.start();

        thread2.rebalanceListener.onPartitionsAssigned(task0Assignment);

        runIt.join();

        assertThat(thread1.tasks().keySet(), equalTo(originalTaskAssignmentThread2));
        assertThat(thread2.tasks().keySet(), equalTo(originalTaskAssignmentThread1));
        assertThat(thread1.prevActiveTasks(), equalTo(originalTaskAssignmentThread1));
        assertThat(thread2.prevActiveTasks(), equalTo(originalTaskAssignmentThread2));
    }

    private class MockStreamsPartitionAssignor extends StreamPartitionAssignor {

        private final Map<TaskId, Set<TopicPartition>> activeTaskAssignment;

        MockStreamsPartitionAssignor(final Map<TaskId, Set<TopicPartition>> activeTaskAssignment) {
            this.activeTaskAssignment = activeTaskAssignment;
        }

        @Override
        Map<TaskId, Set<TopicPartition>> activeTasks() {
            return activeTaskAssignment;
        }

        @Override
        public void close() {}
    }

    @Test
    public void testMetrics() throws Exception {
        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);
        final String defaultGroupName = "stream-metrics";
        final String defaultPrefix = "thread." + thread.threadClientId();
        final Map<String, String> defaultTags = Collections.singletonMap("client-id", thread.threadClientId());

        assertNotNull(metrics.getSensor(defaultPrefix + ".commit-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".poll-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".process-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".punctuate-latency"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".task-created"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".task-closed"));
        assertNotNull(metrics.getSensor(defaultPrefix + ".skipped-records"));

        assertNotNull(metrics.metrics().get(metrics.metricName("commit-latency-avg", defaultGroupName, "The average commit time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("commit-latency-max", defaultGroupName, "The maximum commit time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("commit-rate", defaultGroupName, "The average per-second number of commit calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-avg", defaultGroupName, "The average poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-latency-max", defaultGroupName, "The maximum poll time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("poll-rate", defaultGroupName, "The average per-second number of record-poll calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-avg", defaultGroupName, "The average process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-latency-max", defaultGroupName, "The maximum process time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("process-rate", defaultGroupName, "The average per-second number of process calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-avg", defaultGroupName, "The average punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-latency-max", defaultGroupName, "The maximum punctuate time in ms", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("punctuate-rate", defaultGroupName, "The average per-second number of punctuate calls", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-created-rate", defaultGroupName, "The average per-second number of newly created tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("task-closed-rate", defaultGroupName, "The average per-second number of closed tasks", defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName("skipped-records-rate", defaultGroupName, "The average per-second number of skipped records.", defaultTags)));
    }

    @Test
    public void testMaybeClean() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long cleanupDelay = 1000L;
            final Properties props = configProps(false);
            props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, Long.toString(cleanupDelay));
            props.setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
            final StreamsConfig config = new StreamsConfig(props);
            final File applicationDir = new File(baseDir, applicationId);
            applicationDir.mkdir();
            final File stateDir1 = new File(applicationDir, task1.toString());
            final File stateDir2 = new File(applicationDir, task2.toString());
            final File stateDir3 = new File(applicationDir, task3.toString());
            final File extraDir = new File(applicationDir, applicationId);
            stateDir1.mkdir();
            stateDir2.mkdir();
            stateDir3.mkdir();
            extraDir.mkdir();

            builder.addSource("source1", "topic1");

            final StreamThread thread = new StreamThread(
                builder,
                config,
                clientSupplier,
                applicationId,
                clientId,
                processId,
                metrics,
                mockTime,
                new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                0) {

                @Override
                public void maybeClean(final long now) {
                    super.maybeClean(now);
                }

                @Override
                protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitionsForTask) {
                    final ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(
                        id,
                        applicationId,
                        partitionsForTask,
                        topology,
                        consumer,
                        clientSupplier.getProducer(new HashMap<String, Object>()),
                        restoreConsumer,
                        config,
                        new MockStreamsMetrics(new Metrics()),
                        stateDirectory);
                }
            };

            initPartitionGrouper(config, thread, clientSupplier);
            assertTrue(thread.tasks().isEmpty());
            mockTime.sleep(cleanupDelay);

            // all directories exist since an assignment didn't happen
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertTrue(stateDir3.exists());
            assertTrue(extraDir.exists());

            List<TopicPartition> revokedPartitions;
            List<TopicPartition> assignedPartitions;
            Map<TaskId, StreamTask> prevTasks;

            //
            // Assign t1p1 and t1p2. This should create task1 & task2
            //
            final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
            activeTasks.put(task1, Collections.singleton(t1p1));
            activeTasks.put(task2, Collections.singleton(t1p2));
            thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);
            prevTasks = new HashMap<>(thread.tasks());

            final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;
            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            // there shouldn't be any previous task
            assertTrue(prevTasks.isEmpty());

            // task 1 & 2 are created
            assertEquals(2, thread.tasks().size());

            // all directories should still exit before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean(mockTime.milliseconds());
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertTrue(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories except for task task2 & task3 will be removed. the extra directory should still exists
            mockTime.sleep(11L);
            thread.maybeClean(mockTime.milliseconds());
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

            //
            // Revoke t1p1 and t1p2. This should remove task1 & task2
            //
            activeTasks.clear();

            revokedPartitions = assignedPartitions;
            assignedPartitions = Collections.emptyList();
            prevTasks = new HashMap<>(thread.tasks());

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            // previous tasks should be committed
            assertEquals(2, prevTasks.size());
            for (final StreamTask task : prevTasks.values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task
            assertTrue(thread.tasks().isEmpty());

            // all state directories for task task1 & task2 still exist before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean(mockTime.milliseconds());
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories for task task1 & task2 are removed
            mockTime.sleep(11L);
            thread.maybeClean(mockTime.milliseconds());
            assertFalse(stateDir1.exists());
            assertFalse(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testMaybeCommit() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long commitInterval = 1000L;
            final Properties props = configProps(false);
            props.setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
            props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

            final StreamsConfig config = new StreamsConfig(props);

            builder.addSource("source1", "topic1");

            final StreamThread thread = new StreamThread(
                builder,
                config,
                clientSupplier,
                applicationId,
                clientId,
                processId,
                metrics,
                mockTime,
                new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                0) {

                @Override
                public void maybeCommit(final long now) {
                    super.maybeCommit(now);
                }

                @Override
                protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitionsForTask) {
                    final ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(
                        id,
                        applicationId,
                        partitionsForTask,
                        topology,
                        consumer,
                        clientSupplier.getProducer(new HashMap<String, Object>()),
                        restoreConsumer,
                        config,
                        new MockStreamsMetrics(new Metrics()),
                        stateDirectory);
                }
            };

            initPartitionGrouper(config, thread, clientSupplier);

            final ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

            final List<TopicPartition> revokedPartitions;
            final List<TopicPartition> assignedPartitions;

            //
            // Assign t1p1 and t1p2. This should create Task 1 & 2
            //
            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            assertEquals(2, thread.tasks().size());

            // no task is committed before the commit interval
            mockTime.sleep(commitInterval - 10L);
            thread.maybeCommit(mockTime.milliseconds());
            for (final StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval
            mockTime.sleep(11L);
            thread.maybeCommit(mockTime.milliseconds());
            for (final StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task is committed before the commit interval, again
            mockTime.sleep(commitInterval - 10L);
            thread.maybeCommit(mockTime.milliseconds());
            for (final StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval, again
            mockTime.sleep(11L);
            thread.maybeCommit(mockTime.milliseconds());
            for (final StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() {
        builder.addSource("source1", "someTopic");

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(assignment));

        thread.rebalanceListener.onPartitionsAssigned(Collections.singleton(new TopicPartition("someTopic", 0)));

        assertEquals(1, clientSupplier.producers.size());
        final Producer globalProducer = clientSupplier.producers.get(0);
        assertSame(globalProducer, thread.threadProducer);
        for (final StreamTask task : thread.tasks().values()) {
            assertSame(globalProducer, ((RecordCollectorImpl) task.recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() {
        builder.addSource("source1", "someTopic");

        final MockClientSupplier clientSupplier = new MockClientSupplier(applicationId);
        final StreamThread thread = new StreamThread(
            builder,
            new StreamsConfig(configProps(true)),
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        assignment.put(new TaskId(0, 2), Collections.singleton(new TopicPartition("someTopic", 2)));
        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(assignment));

        final Set<TopicPartition> assignedPartitions = new HashSet<>();
        Collections.addAll(assignedPartitions, new TopicPartition("someTopic", 0), new TopicPartition("someTopic", 2));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertNull(thread.threadProducer);
        assertEquals(thread.tasks().size(), clientSupplier.producers.size());
        final Iterator it = clientSupplier.producers.iterator();
        for (final StreamTask task : thread.tasks().values()) {
            assertSame(it.next(), ((RecordCollectorImpl) task.recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldCloseAllTaskProducersOnCloseIfEosEnabled() {
        builder.addSource("source1", "someTopic");

        final MockClientSupplier clientSupplier = new MockClientSupplier(applicationId);
        final StreamThread thread = new StreamThread(
            builder,
            new StreamsConfig(configProps(true)),
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(assignment));

        thread.rebalanceListener.onPartitionsAssigned(Collections.singleton(new TopicPartition("someTopic", 0)));

        thread.close();
        thread.run();

        for (final StreamTask task : thread.tasks().values()) {
            assertTrue(((MockProducer) ((RecordCollectorImpl) task.recordCollector()).producer()).closed());
        }
    }

    @Test
    public void shouldCloseThreadProducerOnCloseIfEosDisabled() {
        builder.addSource("source1", "someTopic");

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> assignment = new HashMap<>();
        assignment.put(new TaskId(0, 0), Collections.singleton(new TopicPartition("someTopic", 0)));
        assignment.put(new TaskId(0, 1), Collections.singleton(new TopicPartition("someTopic", 1)));
        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(assignment));

        thread.rebalanceListener.onPartitionsAssigned(Collections.singleton(new TopicPartition("someTopic", 0)));

        thread.close();
        thread.run();

        assertTrue(((MockProducer) thread.threadProducer).closed());
    }

    @Test
    public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology() throws Exception {
        builder.addSource("name", "topic").addSink("out", "output");

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return Collections.singletonMap(new TaskId(0, 0), Utils.mkSet(new TopicPartition("topic", 0)));
            }
        });

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldNotCloseSuspendedTaskswice() throws Exception {
        builder.addSource("name", "topic").addSink("out", "output");

        final TestStreamTask testStreamTask = new TestStreamTask(
            new TaskId(0, 0),
            applicationId,
            Utils.mkSet(new TopicPartition("topic", 0)),
            builder.build(0),
            clientSupplier.consumer,
            clientSupplier.getProducer(new HashMap<String, Object>()),
            clientSupplier.restoreConsumer,
            config,
            new MockStreamsMetrics(new Metrics()),
            new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), mockTime));

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitionsForTask) {
                return testStreamTask;
            }
        };

        final Set<TopicPartition> activeTasks = new HashSet<>();
        activeTasks.add(new TopicPartition("topic", 0));

        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return new HashMap<TaskId, Set<TopicPartition>>() {
                    {
                        put(new TaskId(0, 0), activeTasks);
                    }
                };
            }
        });

        thread.rebalanceListener.onPartitionsAssigned(activeTasks);
        thread.rebalanceListener.onPartitionsRevoked(activeTasks);

        assertTrue(testStreamTask.suspended);
        assertFalse(testStreamTask.closed);

        activeTasks.clear();
        // this should succeed without exception
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());

        assertTrue(testStreamTask.closed);
    }
    @Test

    public void shouldInitializeRestoreConsumerWithOffsetsFromStandbyTasks() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey().count("count-one");
        builder.stream("t2").groupByKey().count("count-two");

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-one-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));
        restoreConsumer.updatePartitions("stream-thread-test-count-two-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-two-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final TopicPartition t1 = new TopicPartition("t1", 0);
        standbyTasks.put(new TaskId(0, 0), Utils.mkSet(t1));

        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return standbyTasks;
            }
        });

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());

        assertThat(restoreConsumer.assignment(), equalTo(Utils.mkSet(new TopicPartition("stream-thread-test-count-one-changelog", 0))));

        // assign an existing standby plus a new one
        standbyTasks.put(new TaskId(1, 0), Utils.mkSet(new TopicPartition("t2", 0)));
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());

        assertThat(restoreConsumer.assignment(), equalTo(Utils.mkSet(new TopicPartition("stream-thread-test-count-one-changelog", 0),
                                                                     new TopicPartition("stream-thread-test-count-two-changelog", 0))));
    }

    @Test
    public void shouldCloseSuspendedTasksThatAreNoLongerAssignedToThisStreamThreadBeforeCreatingNewTasks() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey().count("count-one");
        builder.stream("t2").groupByKey().count("count-two");

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);
        final MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-one-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));
        restoreConsumer.updatePartitions("stream-thread-test-count-two-changelog",
                                         Collections.singletonList(new PartitionInfo("stream-thread-test-count-two-changelog",
                                                                                     0,
                                                                                     null,
                                                                                     new Node[0],
                                                                                     new Node[0])));


        final HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        offsets.put(new TopicPartition("stream-thread-test-count-two-changelog", 0), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final TopicPartition t1 = new TopicPartition("t1", 0);
        standbyTasks.put(new TaskId(0, 0), Utils.mkSet(t1));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final TopicPartition t2 = new TopicPartition("t2", 0);
        activeTasks.put(new TaskId(1, 0), Utils.mkSet(t2));

        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return standbyTasks;
            }

            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Utils.mkSet(t2));

        // swap the assignment around and make sure we don't get any exceptions
        standbyTasks.clear();
        activeTasks.clear();
        standbyTasks.put(new TaskId(1, 0), Utils.mkSet(t2));
        activeTasks.put(new TaskId(0, 0), Utils.mkSet(t1));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Utils.mkSet(t1));
    }

    @Test
    public void shouldCloseActiveTasksThatAreAssignedToThisStreamThreadButAssignmentHasChangedBeforeCreatingNewTasks() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream(Pattern.compile("t.*")).to("out");

        final Map<Collection<TopicPartition>, TestStreamTask> createdTasks = new HashMap<>();

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                final ProcessorTopology topology = builder.build(id.topicGroupId);
                final TestStreamTask task = new TestStreamTask(
                    id,
                    applicationId,
                    partitions,
                    topology,
                    consumer,
                    clientSupplier.getProducer(new HashMap<String, Object>()),
                    restoreConsumer,
                    config,
                    new MockStreamsMetrics(new Metrics()),
                    stateDirectory);
                createdTasks.put(partitions, task);
                return task;
            }
        };

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final TopicPartition t1 = new TopicPartition("t1", 0);
        final Set<TopicPartition> task00Partitions = new HashSet<>();
        task00Partitions.add(t1);
        final TaskId taskId = new TaskId(0, 0);
        activeTasks.put(taskId, task00Partitions);

        thread.setPartitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        StreamPartitionAssignor.SubscriptionUpdates subscriptionUpdates = new StreamPartitionAssignor.SubscriptionUpdates();
        Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);
        Set<String> updatedTopics = (Set<String>) updatedTopicsField.get(subscriptionUpdates);
        updatedTopics.add(t1.topic());
        builder.updateSubscriptions(subscriptionUpdates, null);

        // should create task for id 0_0 with a single partition
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(task00Partitions);

        final TestStreamTask firstTask = createdTasks.get(task00Partitions);
        assertThat(firstTask.id(), is(taskId));

        // update assignment for the task 0_0 so it now has 2 partitions
        task00Partitions.add(new TopicPartition("t2", 0));
        updatedTopics.add("t2");

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(task00Partitions);

        // should close the first task as the assignment has changed
        assertTrue("task should have been closed as assignment has changed", firstTask.closed);
        assertTrue("tasks state manager should have been closed as assignment has changed", firstTask.closedStateManager);
        // should have created a new task for 00
        assertThat(createdTasks.get(task00Partitions).id(), is(taskId));
    }

    @Test
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing() throws Exception {
        builder.addSource("source", TOPIC).addSink("sink", "dummyTopic", "source");

        final MockClientSupplier clientSupplier = new MockClientSupplier(applicationId);
        final StreamThread thread = new StreamThread(
            builder,
            new StreamsConfig(configProps(true)),
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final MockConsumer consumer = clientSupplier.consumer;
        consumer.updatePartitions(TOPIC, Collections.singletonList(new PartitionInfo(TOPIC, 0, null, null, null)));

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task1, task0Assignment);

        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(null);
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        assertThat(thread.tasks().size(), equalTo(1));
        final MockProducer producer = clientSupplier.producers.get(0);

        thread.start();

        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return !consumer.subscription().isEmpty();
                }
            },
            "StreamsThread's internal consumer did not subscribe to input topic.");

        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(new HashMap<TopicPartition, Long>() {
            {
                put(task0Assignment.iterator().next(), 0L);
            }
        });
        consumer.unsubscribe();
        consumer.assign(task0Assignment);

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[0], new byte[0]));
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return producer.history().size() == 1;
                }
            },
            "StreamsThread did not produce output record.");

        assertFalse(producer.transactionCommitted());
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return producer.commitCount() == 1;
                }
            },
            "StreamsThread did not commit transaction.");

        producer.fenceProducer();
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[0], new byte[0]));
        TestUtils.waitForCondition(
            new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return thread.tasks().isEmpty();
                }
            },
            "StreamsThread did not remove fenced zombie task.");

        thread.close();
        thread.join();

        assertThat(producer.commitCount(), equalTo(1L));
    }

    @Test
    public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedAtBeginTransactionWhenTaskIsResumed() throws Exception {
        builder.addSource("name", "topic").addSink("out", "output");

        final MockClientSupplier clientSupplier = new MockClientSupplier(applicationId);
        final StreamThread thread = new StreamThread(
            builder,
            new StreamsConfig(configProps(true)),
            clientSupplier,
            applicationId,
            clientId,
            processId,
            new Metrics(),
            new MockTime(),
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(task1, task0Assignment);

        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(null);
        thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
        assertThat(thread.tasks().size(), equalTo(1));

        thread.rebalanceListener.onPartitionsRevoked(null);
        clientSupplier.producers.get(0).fenceProducer();
        try {
            thread.rebalanceListener.onPartitionsAssigned(task0Assignment);
            fail("should have thrown " + ProducerFencedException.class.getSimpleName());
        } catch (final ProducerFencedException e) { }

        assertTrue(thread.tasks().isEmpty());
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenAnExceptionOccursOnTaskCloseDuringShutdown() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();

        final TestStreamTask testStreamTask = new TestStreamTask(
            new TaskId(0, 0),
            applicationId,
            Utils.mkSet(new TopicPartition("t1", 0)),
            builder.build(0),
            clientSupplier.consumer,
            clientSupplier.getProducer(new HashMap<String, Object>()),
            clientSupplier.restoreConsumer,
            config,
            new MockStreamsMetrics(new Metrics()),
            new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), mockTime)) {

            @Override
            public void close(final boolean clean) {
                throw new RuntimeException("KABOOM!");
            }
        };

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };

        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id(), testStreamTask.partitions);

        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);

        thread.start();
        thread.close();
        thread.join();
        assertFalse("task shouldn't have been committed as there was an exception during shutdown", testStreamTask.committed);
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenAnExceptionOccursOnTaskFlushDuringShutdown() throws Exception {
        final MockStateStoreSupplier.MockStateStore stateStore = new MockStateStoreSupplier.MockStateStore("foo", false);
        builder.stream("t1").groupByKey().count(new MockStateStoreSupplier(stateStore));
        final TestStreamTask testStreamTask = new TestStreamTask(
            new TaskId(0, 0),
            applicationId,
            Utils.mkSet(new TopicPartition("t1", 0)),
            builder.build(0),
            clientSupplier.consumer,
            clientSupplier.getProducer(new HashMap<String, Object>()),
            clientSupplier.restoreConsumer,
            config,
            new MockStreamsMetrics(metrics),
            new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), mockTime)) {

            @Override
            public void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id(), testStreamTask.partitions);


        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);
        // store should have been opened
        assertTrue(stateStore.isOpen());

        thread.start();
        thread.close();
        thread.join();
        assertFalse("task shouldn't have been committed as there was an exception during shutdown", testStreamTask.committed);
        // store should be closed even if we had an exception
        assertFalse(stateStore.isOpen());
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();

        final TestStreamTask testStreamTask = new TestStreamTask(
            new TaskId(0, 0),
            applicationId,
            Utils.mkSet(new TopicPartition("t1", 0)),
            builder.build(0),
            clientSupplier.consumer,
            clientSupplier.getProducer(new HashMap<String, Object>()),
            clientSupplier.restoreConsumer,
            config,
            new MockStreamsMetrics(new Metrics()),
            new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), mockTime)) {

            @Override
            public void suspend() {
                throw new RuntimeException("KABOOM!");
            }
        };

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id(), testStreamTask.partitions);


        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);
        try {
            thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
            fail("should have thrown exception");
        } catch (final Exception e) {
            // expected
        }
        assertFalse(testStreamTask.committed);
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushStateWhileSuspendingState() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();

        final TestStreamTask testStreamTask = new TestStreamTask(
            new TaskId(0, 0),
            applicationId,
            Utils.mkSet(new TopicPartition("t1", 0)),
            builder.build(0),
            clientSupplier.consumer,
            clientSupplier.getProducer(new HashMap<String, Object>()),
            clientSupplier.restoreConsumer,
            config,
            new MockStreamsMetrics(new Metrics()),
            new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), mockTime)) {

            @Override
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };

        final StreamThread thread = new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id(), testStreamTask.partitions);


        thread.setPartitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);
        try {
            thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
            fail("should have thrown exception");
        } catch (final Exception e) {
            // expected
        }
        assertFalse(testStreamTask.committed);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldAlwaysUpdateWithLatestTopicsFromStreamPartitionAssignor() throws Exception {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("source", Pattern.compile("t.*"));
        topologyBuilder.addProcessor("processor", new MockProcessorSupplier(), "source");

        final StreamThread thread = new StreamThread(
            topologyBuilder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            mockTime,
            new StreamsMetadataState(topologyBuilder, StreamsMetadataState.UNKNOWN_HOST),
            0);

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        final Map<String, Object> configurationMap = new HashMap<>();

        configurationMap.put(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE, thread);
        configurationMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        partitionAssignor.configure(configurationMap);

        thread.setPartitionAssignor(partitionAssignor);

        final Field
            nodeToSourceTopicsField =
            topologyBuilder.getClass().getDeclaredField("nodeToSourceTopics");
        nodeToSourceTopicsField.setAccessible(true);
        final Map<String, List<String>>
            nodeToSourceTopics =
            (Map<String, List<String>>) nodeToSourceTopicsField.get(topologyBuilder);
        final List<TopicPartition> topicPartitions = new ArrayList<>();

        final TopicPartition topicPartition1 = new TopicPartition("topic-1", 0);
        final TopicPartition topicPartition2 = new TopicPartition("topic-2", 0);
        final TopicPartition topicPartition3 = new TopicPartition("topic-3", 0);

        final TaskId taskId1 = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(0, 0);
        final TaskId taskId3 = new TaskId(0, 0);

        List<TaskId> activeTasks = Arrays.asList(taskId1);

        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());

        topicPartitions.addAll(Arrays.asList(topicPartition1));
        PartitionAssignor.Assignment assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 1);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));

        topicPartitions.clear();

        activeTasks = Arrays.asList(taskId1, taskId2);
        info = new AssignmentInfo(activeTasks, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());
        topicPartitions.addAll(Arrays.asList(topicPartition1, topicPartition2));
        assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 2);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-2"));

        topicPartitions.clear();

        activeTasks = Arrays.asList(taskId1, taskId2, taskId3);
        info = new AssignmentInfo(activeTasks, standbyTasks,
                               new HashMap<HostInfo, Set<TopicPartition>>());
        topicPartitions.addAll(Arrays.asList(topicPartition1, topicPartition2, topicPartition3));
        assignment = new PartitionAssignor.Assignment(topicPartitions, info.encode());
        partitionAssignor.onAssignment(assignment);

        assertTrue(nodeToSourceTopics.get("source").size() == 3);
        assertTrue(nodeToSourceTopics.get("source").contains("topic-1"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-2"));
        assertTrue(nodeToSourceTopics.get("source").contains("topic-3"));

    }

    private void initPartitionGrouper(final StreamsConfig config,
                                      final StreamThread thread,
                                      final MockClientSupplier clientSupplier) {

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        partitionAssignor.configure(config.getConsumerConfigs(thread, thread.applicationId, thread.clientId));
        final MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(thread.config, clientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        final Map<String, PartitionAssignor.Assignment> assignments =
            partitionAssignor.assign(metadata, Collections.singletonMap("client", subscription));

        partitionAssignor.onAssignment(assignments.get("client"));
    }

    private static class StateListenerStub implements StreamThread.StateListener {
        int numChanges = 0;
        StreamThread.State oldState = null;
        StreamThread.State newState = null;

        @Override
        public void onChange(final StreamThread thread, final StreamThread.State newState, final StreamThread.State oldState) {
            ++numChanges;
            if (this.newState != null) {
                if (this.newState != oldState) {
                    throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                }
            }
            this.oldState = oldState;
            this.newState = newState;
        }
    }

    private StreamThread getStreamThread() {
        return new StreamThread(
            builder,
            config,
            clientSupplier,
            applicationId,
            clientId,
            processId,
            metrics,
            Time.SYSTEM,
            new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
            0) {

            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitionsForTask) {
                final ProcessorTopology topology = builder.build(id.topicGroupId);
                return new TestStreamTask(
                    id,
                    applicationId,
                    partitionsForTask,
                    topology,
                    consumer,
                    clientSupplier.getProducer(new HashMap()),
                    restoreConsumer,
                    config,
                    new MockStreamsMetrics(new Metrics()),
                    stateDirectory);
            }
        };
    }
}
