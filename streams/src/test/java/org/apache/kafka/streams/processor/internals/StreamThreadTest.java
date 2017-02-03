/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Before;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.junit.Assert.assertThat;

public class StreamThreadTest {

    private final String clientId = "clientId";
    private final String applicationId = "stream-thread-test";
    private UUID processId = UUID.randomUUID();

    @Before
    public void setUp() throws Exception {
        processId = UUID.randomUUID();
    }

    private TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private TopicPartition t3p2 = new TopicPartition("topic3", 2);

    private List<PartitionInfo> infos = Arrays.asList(
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

    private Cluster metadata = new Cluster("cluster", Arrays.asList(Node.noNode()), infos, Collections.<String>emptySet(),
            Collections.<String>emptySet());

    private final PartitionAssignor.Subscription subscription =
        new PartitionAssignor.Subscription(Arrays.asList("topic1", "topic2", "topic3"), subscriptionUserData());

    private ByteBuffer subscriptionUserData() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer buf = ByteBuffer.allocate(4 + 16 + 4 + 4);
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

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
                setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
            }
        };
    }

    private static class TestStreamTask extends StreamTask {
        public boolean committed = false;
        private boolean closed;
        private boolean closedStateManager;

        public TestStreamTask(TaskId id,
                              String applicationId,
                              Collection<TopicPartition> partitions,
                              ProcessorTopology topology,
                              Consumer<byte[], byte[]> consumer,
                              Producer<byte[], byte[]> producer,
                              Consumer<byte[], byte[]> restoreConsumer,
                              StreamsConfig config,
                              StreamsMetrics metrics,
                              StateDirectory stateDirectory) {
            super(id, applicationId, partitions, topology, consumer, restoreConsumer, config, metrics,
                stateDirectory, null, new MockTime(), new RecordCollectorImpl(producer, id.toString()));
        }

        @Override
        public void commit() {
            super.commit();
            committed = true;
        }

        @Override
        public void commitOffsets() {
            super.commitOffsets();
            committed = true;
        }

        @Override
        protected void initializeOffsetLimits() {
            // do nothing
        }

        @Override
        public void close() {
            this.closed = true;
            super.close();
        }

        @Override
        void closeStateManager(boolean writeCheckpoint) {
            super.closeStateManager(writeCheckpoint);
            this.closedStateManager = true;
        }
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChange() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());
        StateListenerStub stateListener = new StateListenerStub();


        TopologyBuilder builder = new TopologyBuilder().setApplicationId("X");
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addSource("source3", "topic3");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source2", "source3");


        MockClientSupplier mockClientSupplier = new MockClientSupplier();
        StreamThread thread = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {

            @Override
            protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {

                ProcessorTopology topology = builder.build(id.topicGroupId);
                return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer,
                    producer, restoreConsumer, config, new MockStreamsMetrics(new Metrics()), stateDirectory);
            }
        };

        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        initPartitionGrouper(config, thread, mockClientSupplier);

        ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        assertTrue(thread.tasks().isEmpty());

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;
        Set<TopicPartition> expectedGroup1;
        Set<TopicPartition> expectedGroup2;

        revokedPartitions = Collections.emptyList();
        assignedPartitions = Collections.singletonList(t1p1);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assertEquals(thread.state(), StreamThread.State.PARTITIONS_REVOKED);
        Assert.assertEquals(stateListener.numChanges, 1);
        Assert.assertEquals(stateListener.oldState, StreamThread.State.RUNNING);
        Assert.assertEquals(stateListener.newState, StreamThread.State.PARTITIONS_REVOKED);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        Assert.assertEquals(stateListener.numChanges, 3);
        Assert.assertEquals(stateListener.oldState, StreamThread.State.ASSIGNING_PARTITIONS);
        Assert.assertEquals(stateListener.newState, StreamThread.State.RUNNING);

        assertTrue(thread.tasks().containsKey(task1));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        assertFalse(thread.tasks().containsKey(task1));
        assertEquals(0, thread.tasks().size());
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t1p2);
        expectedGroup1 = new HashSet<>(Collections.singleton(t1p1));
        expectedGroup2 = new HashSet<>(Collections.singleton(t1p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task2));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task2).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t2p1, t2p2, t3p1, t3p2);
        expectedGroup1 = new HashSet<>(Arrays.asList(t2p1, t3p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p2, t3p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task4));
        assertTrue(thread.tasks().containsKey(task5));
        assertEquals(expectedGroup1, thread.tasks().get(task4).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task5).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task4));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task4).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Arrays.asList(t1p1, t2p1, t3p1);
        expectedGroup1 = new HashSet<>(Arrays.asList(t1p1));
        expectedGroup2 = new HashSet<>(Arrays.asList(t2p1, t3p1));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertTrue(thread.tasks().containsKey(task4));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(expectedGroup2, thread.tasks().get(task4).partitions());
        assertEquals(2, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.emptyList();

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().isEmpty());

        thread.close();
        assertTrue((thread.state() == StreamThread.State.PENDING_SHUTDOWN) ||
            (thread.state() == StreamThread.State.NOT_RUNNING));
    }

    final static String TOPIC = "topic";
    final Set<TopicPartition> task0Assignment = Collections.singleton(new TopicPartition(TOPIC, 0));
    final Set<TopicPartition> task1Assignment = Collections.singleton(new TopicPartition(TOPIC, 1));

    @Test
    public void testHandingOverTaskFromOneToAnotherThread() throws Exception {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addStateStore(
            Stores
                .create("store")
                .withByteArrayKeys()
                .withByteArrayValues()
                .persistent()
                .build()
        );
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier mockClientSupplier = new MockClientSupplier();
        mockClientSupplier.consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));

        final StreamThread thread1 = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId + 1, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);
        final StreamThread thread2 = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId + 2, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);

        final Map<TaskId, Set<TopicPartition>> task0 = Collections.singletonMap(new TaskId(0, 0), task0Assignment);
        final Map<TaskId, Set<TopicPartition>> task1 = Collections.singletonMap(new TaskId(0, 1), task1Assignment);

        final Map<TaskId, Set<TopicPartition>> thread1Assignment = new HashMap<>(task0);
        final Map<TaskId, Set<TopicPartition>> thread2Assignment = new HashMap<>(task1);

        thread1.partitionAssignor(new MockStreamsPartitionAssignor(thread1Assignment));
        thread2.partitionAssignor(new MockStreamsPartitionAssignor(thread2Assignment));

        // revoke (to get threads in correct state)
        thread1.rebalanceListener.onPartitionsRevoked(Collections.EMPTY_SET);
        thread2.rebalanceListener.onPartitionsRevoked(Collections.EMPTY_SET);

        // assign
        thread1.rebalanceListener.onPartitionsAssigned(task0Assignment);
        thread2.rebalanceListener.onPartitionsAssigned(task1Assignment);

        final Set<TaskId> originalTaskAssignmentThread1 = new HashSet<>();
        for (TaskId tid : thread1.tasks().keySet()) {
            originalTaskAssignmentThread1.add(tid);
        }
        final Set<TaskId> originalTaskAssignmentThread2 = new HashSet<>();
        for (TaskId tid : thread2.tasks().keySet()) {
            originalTaskAssignmentThread2.add(tid);
        }

        // revoke (task will be suspended)
        thread1.rebalanceListener.onPartitionsRevoked(task0Assignment);
        thread2.rebalanceListener.onPartitionsRevoked(task1Assignment);


        // assign reverted
        thread1Assignment.clear();
        thread1Assignment.putAll(task1);

        thread2Assignment.clear();
        thread2Assignment.putAll(task0);

        Thread runIt = new Thread(new Runnable() {
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

        public MockStreamsPartitionAssignor(final Map<TaskId, Set<TopicPartition>> activeTaskAssignment) {
            this.activeTaskAssignment = activeTaskAssignment;
        }

        @Override
        Map<TaskId, Set<TopicPartition>> activeTasks() {
            return activeTaskAssignment;
        }
    }

    @Test
    public void testMetrics() throws Exception {
        TopologyBuilder builder = new TopologyBuilder().setApplicationId("MetricsApp");
        StreamsConfig config = new StreamsConfig(configProps());
        MockClientSupplier clientSupplier = new MockClientSupplier();

        Metrics metrics = new Metrics();
        StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
            clientId,  processId, metrics, new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);
        String defaultGroupName = "stream-metrics";
        String defaultPrefix = "thread." + thread.threadClientId();
        Map<String, String> defaultTags = Collections.singletonMap("client-id", thread.threadClientId());

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
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long cleanupDelay = 1000L;
            Properties props = configProps();
            props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, Long.toString(cleanupDelay));
            props.setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());

            StreamsConfig config = new StreamsConfig(props);

            File applicationDir = new File(baseDir, applicationId);
            applicationDir.mkdir();
            File stateDir1 = new File(applicationDir, task1.toString());
            File stateDir2 = new File(applicationDir, task2.toString());
            File stateDir3 = new File(applicationDir, task3.toString());
            File extraDir = new File(applicationDir, "X");
            stateDir1.mkdir();
            stateDir2.mkdir();
            stateDir3.mkdir();
            extraDir.mkdir();

            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder().setApplicationId("X");
            builder.addSource("source1", "topic1");

            MockClientSupplier mockClientSupplier = new MockClientSupplier();
            StreamThread thread = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId, processId, new Metrics(), mockTime, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                                                   0) {

                @Override
                public void maybeClean() {
                    super.maybeClean();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer,
                        producer, restoreConsumer, config, new MockStreamsMetrics(new Metrics()), stateDirectory);
                }
            };

            initPartitionGrouper(config, thread, mockClientSupplier);

            ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

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
            thread.partitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);
            prevTasks = new HashMap<>(thread.tasks());

            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            rebalanceListener.onPartitionsAssigned(assignedPartitions);

            // there shouldn't be any previous task
            assertTrue(prevTasks.isEmpty());

            // task 1 & 2 are created
            assertEquals(2, thread.tasks().size());

            // all directories should still exit before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean();
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertTrue(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories except for task task2 & task3 will be removed. the extra directory should still exists
            mockTime.sleep(11L);
            thread.maybeClean();
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
            for (StreamTask task : prevTasks.values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task
            assertTrue(thread.tasks().isEmpty());

            // all state directories for task task1 & task2 still exist before the cleanup delay time
            mockTime.sleep(cleanupDelay - 10L);
            thread.maybeClean();
            assertTrue(stateDir1.exists());
            assertTrue(stateDir2.exists());
            assertFalse(stateDir3.exists());
            assertTrue(extraDir.exists());

            // all state directories for task task1 & task2 are removed
            mockTime.sleep(11L);
            thread.maybeClean();
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
        File baseDir = Files.createTempDirectory("test").toFile();
        try {
            final long commitInterval = 1000L;
            Properties props = configProps();
            props.setProperty(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath());
            props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(commitInterval));

            StreamsConfig config = new StreamsConfig(props);

            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder().setApplicationId("X");
            builder.addSource("source1", "topic1");

            MockClientSupplier mockClientSupplier = new MockClientSupplier();
            StreamThread thread = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId, processId, new Metrics(), mockTime, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                                                   0) {

                @Override
                public void maybeCommit() {
                    super.maybeCommit();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer,
                        producer, restoreConsumer, config, new MockStreamsMetrics(new Metrics()), stateDirectory);
                }
            };

            initPartitionGrouper(config, thread, mockClientSupplier);

            ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

            List<TopicPartition> revokedPartitions;
            List<TopicPartition> assignedPartitions;

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
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval
            mockTime.sleep(11L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

            // no task is committed before the commit interval, again
            mockTime.sleep(commitInterval - 10L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertFalse(((TestStreamTask) task).committed);
            }

            // all tasks are committed after the commit interval, again
            mockTime.sleep(11L);
            thread.maybeCommit();
            for (StreamTask task : thread.tasks().values()) {
                assertTrue(((TestStreamTask) task).committed);
                ((TestStreamTask) task).committed = false;
            }

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testInjectClients() {
        TopologyBuilder builder = new TopologyBuilder().setApplicationId("X");
        StreamsConfig config = new StreamsConfig(configProps());
        MockClientSupplier clientSupplier = new MockClientSupplier();
        StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
                                               clientId, processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                                               0);
        assertSame(clientSupplier.producer, thread.producer);
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    @Test
    public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology() throws Exception {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId(applicationId)
                .addSource("name", "topic")
                .addSink("out", "output");


        final StreamsConfig config = new StreamsConfig(configProps());
        final StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), applicationId,
                                               clientId, processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);

        thread.partitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> standbyTasks() {
                return Collections.singletonMap(new TaskId(0, 0), Utils.mkSet(new TopicPartition("topic", 0)));
            }
        });

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldInitializeRestoreConsumerWithOffsetsFromStandbyTasks() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey().count("count-one");
        builder.stream("t2").groupByKey().count("count-two");
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);

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

        thread.partitionAssignor(new StreamPartitionAssignor() {
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
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0);
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

        thread.partitionAssignor(new StreamPartitionAssignor() {
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
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final Map<Collection<TopicPartition>, TestStreamTask> createdTasks = new HashMap<>();

        final StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {
            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                final ProcessorTopology topology = builder.build(id.topicGroupId);
                final TestStreamTask task = new TestStreamTask(id, applicationId, partitions, topology, consumer,
                    producer, restoreConsumer, config, new MockStreamsMetrics(new Metrics()), stateDirectory);
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

        thread.partitionAssignor(new StreamPartitionAssignor() {
            @Override
            Map<TaskId, Set<TopicPartition>> activeTasks() {
                return activeTasks;
            }
        });

        // should create task for id 0_0 with a single partition
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(task00Partitions);

        final TestStreamTask firstTask = createdTasks.get(task00Partitions);
        assertThat(firstTask.id(), is(taskId));

        // update assignment for the task 0_0 so it now has 2 partitions
        task00Partitions.add(new TopicPartition("t2", 0));
        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(task00Partitions);

        // should close the first task as the assignment has changed
        assertTrue("task should have been closed as assignment has changed", firstTask.closed);
        assertTrue("tasks state manager should have been closed as assignment has changed", firstTask.closedStateManager);
        // should have created a new task for 00
        assertThat(createdTasks.get(task00Partitions).id(), is(taskId));
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenAnExceptionOccursOnTaskCloseDuringShutdown() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        final TestStreamTask testStreamTask = new TestStreamTask(new TaskId(0, 0),
                                                                 applicationId,
                                                                 Utils.mkSet(new TopicPartition("t1", 0)),
                                                                 builder.build(0),
                                                                 clientSupplier.consumer,
                                                                 clientSupplier.producer,
                                                                 clientSupplier.restoreConsumer,
                                                                 config,
                                                                 new MockStreamsMetrics(new Metrics()),
                                                                 new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG))) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final StreamsConfig config1 = new StreamsConfig(configProps());

        final StreamThread thread = new StreamThread(builder, config1, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(),
                                                     new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {
            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id, testStreamTask.partitions);


        thread.partitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);

        thread.start();
        thread.close();
        thread.join();
        assertFalse("task shouldn't have been committed as there was an exception during shutdown", testStreamTask.committed);


    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenAnExceptionOccursOnTaskFlushDuringShutdown() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        final MockStateStoreSupplier.MockStateStore stateStore = new MockStateStoreSupplier.MockStateStore("foo", false);
        builder.stream("t1").groupByKey().count(new MockStateStoreSupplier(stateStore));
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        final TestStreamTask testStreamTask = new TestStreamTask(new TaskId(0, 0),
                                                                 applicationId,
                                                                 Utils.mkSet(new TopicPartition("t1", 0)),
                                                                 builder.build(0),
                                                                 clientSupplier.consumer,
                                                                 clientSupplier.producer,
                                                                 clientSupplier.restoreConsumer,
                                                                 config,
                                                                 new MockStreamsMetrics(new Metrics()),
                                                                 new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG))) {
            @Override
            public void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };

        final StreamThread thread = new StreamThread(builder, config, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(),
                                                     new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {
            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id, testStreamTask.partitions);


        thread.partitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

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
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringCloseTopologyWhenSuspendingState() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        final TestStreamTask testStreamTask = new TestStreamTask(new TaskId(0, 0),
                                                                 applicationId,
                                                                 Utils.mkSet(new TopicPartition("t1", 0)),
                                                                 builder.build(0),
                                                                 clientSupplier.consumer,
                                                                 clientSupplier.producer,
                                                                 clientSupplier.restoreConsumer,
                                                                 config,
                                                                 new MockStreamsMetrics(new Metrics()),
                                                                 new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG))) {
            @Override
            public void closeTopology() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final StreamsConfig config1 = new StreamsConfig(configProps());

        final StreamThread thread = new StreamThread(builder, config1, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(),
                                                     new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {
            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id, testStreamTask.partitions);


        thread.partitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);
        try {
            thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
            fail("should have thrown exception");
        } catch (Exception e) {
            // expected
        }
        assertFalse(testStreamTask.committed);
    }

    @Test
    public void shouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushStateWhileSuspendingState() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId(applicationId);
        builder.stream("t1").groupByKey();
        final StreamsConfig config = new StreamsConfig(configProps());
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        final TestStreamTask testStreamTask = new TestStreamTask(new TaskId(0, 0),
                                                                 applicationId,
                                                                 Utils.mkSet(new TopicPartition("t1", 0)),
                                                                 builder.build(0),
                                                                 clientSupplier.consumer,
                                                                 clientSupplier.producer,
                                                                 clientSupplier.restoreConsumer,
                                                                 config,
                                                                 new MockStreamsMetrics(new Metrics()),
                                                                 new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG))) {
            @Override
            public void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final StreamsConfig config1 = new StreamsConfig(configProps());

        final StreamThread thread = new StreamThread(builder, config1, clientSupplier, applicationId,
                                                     clientId, processId, new Metrics(), new MockTime(),
                                                     new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST), 0) {
            @Override
            protected StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
                return testStreamTask;
            }
        };


        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(testStreamTask.id, testStreamTask.partitions);


        thread.partitionAssignor(new MockStreamsPartitionAssignor(activeTasks));

        thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
        thread.rebalanceListener.onPartitionsAssigned(testStreamTask.partitions);
        try {
            thread.rebalanceListener.onPartitionsRevoked(Collections.<TopicPartition>emptyList());
            fail("should have thrown exception");
        } catch (Exception e) {
            // expected
        }
        assertFalse(testStreamTask.committed);

    }


    private void initPartitionGrouper(StreamsConfig config, StreamThread thread, MockClientSupplier clientSupplier) {

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        partitionAssignor.configure(config.getConsumerConfigs(thread, thread.applicationId, thread.clientId));
        MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(thread.config, clientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        Map<String, PartitionAssignor.Assignment> assignments =
            partitionAssignor.assign(metadata, Collections.singletonMap("client", subscription));

        partitionAssignor.onAssignment(assignments.get("client"));
    }

    public static class StateListenerStub implements StreamThread.StateListener {
        public int numChanges = 0;
        public StreamThread.State oldState = null;
        public StreamThread.State newState = null;

        @Override
        public void onChange(final StreamThread thread, final StreamThread.State newState, final StreamThread.State oldState) {
            this.numChanges++;
            if (this.newState != null) {
                if (this.newState != oldState) {
                    throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                }
            }
            this.oldState = oldState;
            this.newState = newState;
        }
    }
}
