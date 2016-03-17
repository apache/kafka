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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
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

    private Cluster metadata = new Cluster(Arrays.asList(Node.noNode()), infos, Collections.<String>emptySet());

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
                              StreamsConfig config) {
            super(id, applicationId, partitions, topology, consumer, producer, restoreConsumer, config, null);
        }

        @Override
        public void commit() {
            super.commit();
            committed = true;
        }

        @Override
        protected void initializeOffsetLimits() {
            // do nothing
        }
    }

    private ByteArraySerializer serializer = new ByteArraySerializer();

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionAssignmentChange() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addSource("source3", "topic3");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source2", "source3");

        StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, applicationId, clientId, processId, new Metrics(), new SystemTime()) {
            @Override
            protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                ProcessorTopology topology = builder.build("X", id.topicGroupId);
                return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, mockRestoreConsumer, config);
            }
        };

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
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().containsKey(task1));
        assertEquals(expectedGroup1, thread.tasks().get(task1).partitions());
        assertEquals(1, thread.tasks().size());

        revokedPartitions = assignedPartitions;
        assignedPartitions = Collections.singletonList(t1p2);
        expectedGroup2 = new HashSet<>(Arrays.asList(t1p2));

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
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
        assignedPartitions = Collections.emptyList();

        rebalanceListener.onPartitionsRevoked(revokedPartitions);
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertTrue(thread.tasks().isEmpty());
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

            MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
            MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder();
            builder.addSource("source1", "topic1");

            StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, applicationId, clientId,  processId, new Metrics(), mockTime) {
                @Override
                public void maybeClean() {
                    super.maybeClean();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build("X", id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, mockRestoreConsumer, config);
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
            Map<Integer, StreamTask> prevTasks;

            //
            // Assign t1p1 and t1p2. This should create task1 & task2
            //
            revokedPartitions = Collections.emptyList();
            assignedPartitions = Arrays.asList(t1p1, t1p2);
            prevTasks = new HashMap(thread.tasks());

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
            prevTasks = new HashMap(thread.tasks());

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

            MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
            MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            final MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            MockTime mockTime = new MockTime();

            TopologyBuilder builder = new TopologyBuilder();
            builder.addSource("source1", "topic1");

            StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, applicationId, clientId,  processId, new Metrics(), mockTime) {
                @Override
                public void maybeCommit() {
                    super.maybeCommit();
                }

                @Override
                protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
                    ProcessorTopology topology = builder.build("X", id.topicGroupId);
                    return new TestStreamTask(id, applicationId, partitionsForTask, topology, consumer, producer, mockRestoreConsumer, config);
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

    private void initPartitionGrouper(StreamsConfig config, StreamThread thread) {

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        partitionAssignor.configure(config.getConsumerConfigs(thread, thread.applicationId, thread.clientId));

        Map<String, PartitionAssignor.Assignment> assignments =
                partitionAssignor.assign(metadata, Collections.singletonMap("client", subscription));

        partitionAssignor.onAssignment(assignments.get("client"));
    }
}
