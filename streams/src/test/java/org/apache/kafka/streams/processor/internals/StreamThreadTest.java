/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StreamThreadTest {

    private final String clientId = "clientId";
    private final String applicationId = "stream-thread-test";
    private final UUID processId = UUID.randomUUID();

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

        public TestStreamTask(TaskId id,
                              String applicationId,
                              Collection<TopicPartition> partitions,
                              ProcessorTopology topology,
                              Consumer<byte[], byte[]> consumer,
                              Producer<byte[], byte[]> producer,
                              Consumer<byte[], byte[]> restoreConsumer,
                              StreamsConfig config,
                              StateDirectory stateDirectory) {
            super(id, applicationId, partitions, topology, consumer, producer, restoreConsumer, config, null, stateDirectory, null);
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

        StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), applicationId, clientId, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder)) {
            @Override
            protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                ProcessorTopology topology = builder.build(id.topicGroupId);
                return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, restoreConsumer, config, stateDirectory);
            }
        };
        thread.setStateListener(stateListener);
        assertEquals(thread.state(), StreamThread.State.RUNNING);
        initPartitionGrouper(config, thread);

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
    final Set<TopicPartition> assignmentThread1 = Collections.singleton(new TopicPartition(TOPIC, 0));
    final Set<TopicPartition> assignmentThread2 = Collections.singleton(new TopicPartition(TOPIC, 1));

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

        final StreamThread thread1 = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId + 1, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder));
        final StreamThread thread2 = new StreamThread(builder, config, mockClientSupplier, applicationId, clientId + 2, processId, new Metrics(), Time.SYSTEM, new StreamsMetadataState(builder));
        thread1.partitionAssignor(new MockStreamsPartitionAssignor());
        thread2.partitionAssignor(new MockStreamsPartitionAssignor());

        // revoke (to get threads in correct state)
        thread1.rebalanceListener.onPartitionsRevoked(Collections.EMPTY_SET);
        thread2.rebalanceListener.onPartitionsRevoked(Collections.EMPTY_SET);

        // assign
        thread1.rebalanceListener.onPartitionsAssigned(assignmentThread1);
        thread2.rebalanceListener.onPartitionsAssigned(assignmentThread2);

        final Set<TaskId> originalTaskAssignmentThread1 = new HashSet<>();
        for (TaskId tid : thread1.tasks().keySet()) {
            originalTaskAssignmentThread1.add(tid);
        }
        final Set<TaskId> originalTaskAssignmentThread2 = new HashSet<>();
        for (TaskId tid : thread2.tasks().keySet()) {
            originalTaskAssignmentThread2.add(tid);
        }

        // revoke (task will be suspended)
        thread1.rebalanceListener.onPartitionsRevoked(assignmentThread1);
        thread2.rebalanceListener.onPartitionsRevoked(assignmentThread2);

        // assign reverted
        Thread runIt = new Thread(new Runnable() {
            @Override
            public void run() {
                thread1.rebalanceListener.onPartitionsAssigned(assignmentThread2);
            }
        });
        runIt.start();

        thread2.rebalanceListener.onPartitionsAssigned(assignmentThread1);

        runIt.join();

        assertThat(thread1.tasks().keySet(), equalTo(originalTaskAssignmentThread2));
        assertThat(thread2.tasks().keySet(), equalTo(originalTaskAssignmentThread1));
        assertThat(thread1.prevTasks(), equalTo(originalTaskAssignmentThread1));
        assertThat(thread2.prevTasks(), equalTo(originalTaskAssignmentThread2));
    }

    private class MockStreamsPartitionAssignor extends StreamPartitionAssignor {
        @Override
        Map<TaskId, Set<TopicPartition>> activeTasks() {
            Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
            activeTasks.put(new TaskId(0, 0), assignmentThread1);
            activeTasks.put(new TaskId(0, 1), assignmentThread2);
            return activeTasks;
        }
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

            StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), applicationId, clientId,  processId, new Metrics(), mockTime, new StreamsMetadataState(builder)) {
                @Override
                public void maybeClean() {
                    super.maybeClean();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, restoreConsumer, config, stateDirectory);
                }
            };

            initPartitionGrouper(config, thread);

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

            StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), applicationId, clientId,  processId, new Metrics(), mockTime, new StreamsMetadataState(builder)) {
                @Override
                public void maybeCommit() {
                    super.maybeCommit();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build(id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, restoreConsumer, config, stateDirectory);
                }
            };

            initPartitionGrouper(config, thread);

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
                                               clientId,  processId, new Metrics(), new MockTime(), new StreamsMetadataState(builder));
        assertSame(clientSupplier.producer, thread.producer);
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    private void initPartitionGrouper(StreamsConfig config, StreamThread thread) {

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        partitionAssignor.configure(config.getConsumerConfigs(thread, thread.applicationId, thread.clientId));

        Map<String, PartitionAssignor.Assignment> assignments =
                partitionAssignor.assign(metadata, Collections.singletonMap("client", subscription));

        partitionAssignor.onAssignment(assignments.get("client"));
    }

    public static class StateListenerStub implements StreamThread.StateListener {
        public int numChanges = 0;
        public StreamThread.State oldState = null;
        public StreamThread.State newState = null;

        @Override
        public void onChange(final StreamThread.State newState, final StreamThread.State oldState) {
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
