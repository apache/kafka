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

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class KafkaStreamingPartitionAssignorTest {

    private TopicPartition t1p0 = new TopicPartition("topic1", 0);
    private TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private TopicPartition t2p0 = new TopicPartition("topic2", 0);
    private TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private TopicPartition t2p3 = new TopicPartition("topic2", 3);

    private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0])
    );

    private Cluster metadata = new Cluster(Arrays.asList(Node.noNode()), infos, Collections.<String>emptySet());

    private ByteBuffer subscriptionUserData() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer buf = ByteBuffer.allocate(4 + 16 + 4 + 4);
        // version
        buf.putInt(1);
        // encode client clientUUID
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        // previously running tasks
        buf.putInt(0);
        // cached tasks
        buf.putInt(0);
        buf.rewind();
        return buf;
    }

    private final TaskId task0 = new TaskId(0, 0);
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(0, 3);

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.test.MockTimestampExtractor");
                setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
            }
        };
    }

    private ByteArraySerializer serializer = new ByteArraySerializer();

    @SuppressWarnings("unchecked")
    @Test
    public void testSubscription() throws Exception {
        StreamingConfig config = new StreamingConfig(configProps());

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        final Set<TaskId> prevTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> cachedTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
                new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        UUID uuid = UUID.randomUUID();
        StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", uuid, new Metrics(), new SystemTime()) {
            @Override
            public Set<TaskId> prevTasks() {
                return prevTasks;
            }
            @Override
            public Set<TaskId> cachedTasks() {
                return cachedTasks;
            }
        };

        KafkaStreamingPartitionAssignor partitionAssignor = new KafkaStreamingPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread));

        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("topic1", "topic2"));

        Collections.sort(subscription.topics());
        assertEquals(Utils.mkList("topic1", "topic2"), subscription.topics());

        Set<TaskId> standbyTasks = new HashSet<>(cachedTasks);
        standbyTasks.removeAll(prevTasks);

        SubscriptionInfo info = new SubscriptionInfo(uuid, prevTasks, standbyTasks);
        assertEquals(info.encode(), subscription.userData());
    }

    @Test
    public void testAssign() throws Exception {
        StreamingConfig config = new StreamingConfig(configProps());

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        List<String> topics = Utils.mkList("topic1", "topic2");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        final Set<TaskId> prevTasks10 = Utils.mkSet(task0);
        final Set<TaskId> prevTasks11 = Utils.mkSet(task1);
        final Set<TaskId> prevTasks20 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks10 = Utils.mkSet(task1);
        final Set<TaskId> standbyTasks11 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks20 = Utils.mkSet(task0);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        StreamThread thread10 = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", uuid1, new Metrics(), new SystemTime());

        KafkaStreamingPartitionAssignor partitionAssignor = new KafkaStreamingPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks11, standbyTasks11).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks20, standbyTasks20).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assigned partitions

        assertEquals(Utils.mkSet(Utils.mkSet(t1p0, t2p0), Utils.mkSet(t1p1, t2p1)),
                Utils.mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));
        assertEquals(Utils.mkSet(t1p2, t2p2), new HashSet<>(assignments.get("consumer20").partitions()));

        // check assignment info
        Set<TaskId> allActiveTasks = new HashSet<>();
        AssignmentInfo info;

        List<TaskId> activeTasks = new ArrayList<>();
        for (TopicPartition partition : assignments.get("consumer10").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer10").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());
        assertEquals(0, info.standbyTasks.size());

        allActiveTasks.addAll(info.activeTasks);

        activeTasks.clear();
        for (TopicPartition partition : assignments.get("consumer11").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer11").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());
        assertEquals(0, info.standbyTasks.size());

        allActiveTasks.addAll(info.activeTasks);

        // check active tasks assigned to the first client
        assertEquals(Utils.mkSet(task0, task1), new HashSet<>(allActiveTasks));

        activeTasks.clear();
        for (TopicPartition partition : assignments.get("consumer20").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer20").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());
        assertEquals(0, info.standbyTasks.size());

        allActiveTasks.addAll(info.activeTasks);

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));
    }

    @Test
    public void testAssignWithStandbyReplicas() throws Exception {
        Properties props = configProps();
        props.setProperty(StreamingConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        StreamingConfig config = new StreamingConfig(props);

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        List<String> topics = Utils.mkList("topic1", "topic2");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        final Set<TaskId> prevTasks10 = Utils.mkSet(task0);
        final Set<TaskId> prevTasks11 = Utils.mkSet(task1);
        final Set<TaskId> prevTasks20 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks10 = Utils.mkSet(task1);
        final Set<TaskId> standbyTasks11 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks20 = Utils.mkSet(task0);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        StreamThread thread10 = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", uuid1, new Metrics(), new SystemTime());

        KafkaStreamingPartitionAssignor partitionAssignor = new KafkaStreamingPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks11, standbyTasks11).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks20, standbyTasks20).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assigned partitions

        assertEquals(Utils.mkSet(Utils.mkSet(t1p0, t2p0), Utils.mkSet(t1p1, t2p1)),
                Utils.mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));
        assertEquals(Utils.mkSet(t1p2, t2p2), new HashSet<>(assignments.get("consumer20").partitions()));

        // check assignment info
        Set<TaskId> allActiveTasks = new HashSet<>();
        Set<TaskId> allStandbyTasks = new HashSet<>();
        AssignmentInfo info;

        List<TaskId> activeTasks = new ArrayList<>();
        for (TopicPartition partition : assignments.get("consumer10").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer10").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());

        allActiveTasks.addAll(info.activeTasks);
        allStandbyTasks.addAll(info.standbyTasks);

        activeTasks.clear();
        for (TopicPartition partition : assignments.get("consumer11").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer11").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());

        allActiveTasks.addAll(info.activeTasks);
        allStandbyTasks.addAll(info.standbyTasks);

        // check tasks assigned to the first client
        assertEquals(Utils.mkSet(task0, task1), new HashSet<>(allActiveTasks));

        activeTasks.clear();
        for (TopicPartition partition : assignments.get("consumer20").partitions()) {
            activeTasks.add(new TaskId(0, partition.partition()));
        }
        info = AssignmentInfo.decode(assignments.get("consumer20").userData());
        assertEquals(activeTasks, info.activeTasks);
        assertEquals(2, info.activeTasks.size());
        assertEquals(1, new HashSet<>(info.activeTasks).size());

        allActiveTasks.addAll(info.activeTasks);
        allStandbyTasks.addAll(info.standbyTasks);

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allStandbyTasks.size());
        assertEquals(allTasks, new HashSet<>(allStandbyTasks));
    }

    @Test
    public void testOnAssignment() throws Exception {
        StreamingConfig config = new StreamingConfig(configProps());

        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, serializer, serializer);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<byte[], byte[]> mockRestoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        UUID uuid = UUID.randomUUID();

        StreamThread thread = new StreamThread(builder, config, producer, consumer, mockRestoreConsumer, "test", uuid, new Metrics(), new SystemTime());

        KafkaStreamingPartitionAssignor partitionAssignor = new KafkaStreamingPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread));

        List<TaskId> activeTaskList = Utils.mkList(task0, task3);
        Set<TaskId> standbyTasks = Utils.mkSet(task1, task2);
        AssignmentInfo info = new AssignmentInfo(activeTaskList, standbyTasks);
        PartitionAssignor.Assignment assignment = new PartitionAssignor.Assignment(Utils.mkList(t1p0, t2p3), info.encode());
        partitionAssignor.onAssignment(assignment);

        assertEquals(Utils.mkSet(task0), partitionAssignor.taskIds(t1p0));
        assertEquals(Utils.mkSet(task3), partitionAssignor.taskIds(t2p3));
        assertEquals(standbyTasks, partitionAssignor.standbyTasks());
    }

}
