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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStoreBuilder;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class StreamsPartitionAssignorTest {

    private final TopicPartition t1p0 = new TopicPartition("topic1", 0);
    private final TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private final TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private final TopicPartition t1p3 = new TopicPartition("topic1", 3);
    private final TopicPartition t2p0 = new TopicPartition("topic2", 0);
    private final TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private final TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private final TopicPartition t2p3 = new TopicPartition("topic2", 3);
    private final TopicPartition t3p0 = new TopicPartition("topic3", 0);
    private final TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private final TopicPartition t3p2 = new TopicPartition("topic3", 2);
    private final TopicPartition t3p3 = new TopicPartition("topic3", 3);

    private final Set<String> allTopics = Utils.mkSet("topic1", "topic2");

    private final List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic3", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic3", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic3", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic3", 3, Node.noNode(), new Node[0], new Node[0])
    );

    private final Cluster metadata = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        infos, Collections.<String>emptySet(),
        Collections.<String>emptySet());

    private final TaskId task0 = new TaskId(0, 0);
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(0, 3);
    private final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private final String userEndPoint = "localhost:8080";
    private final String applicationId = "stream-partition-assignor-test";

    private final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);

    private Map<String, Object> configProps() {
        Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, userEndPoint);
        configurationMap.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        return configurationMap;
    }

    private void configurePartitionAssignor(final Map<String, Object> props) {
        Map<String, Object> configurationMap = configProps();
        configurationMap.putAll(props);
        partitionAssignor.configure(configurationMap);
    }

    private void mockTaskManager(final Set<TaskId> prevTasks,
                                 final Set<TaskId> cachedTasks,
                                 final UUID processId,
                                 final InternalTopologyBuilder builder) {
        EasyMock.expect(taskManager.builder()).andReturn(builder).anyTimes();
        EasyMock.expect(taskManager.prevActiveTaskIds()).andReturn(prevTasks).anyTimes();
        EasyMock.expect(taskManager.cachedTasksIds()).andReturn(cachedTasks).anyTimes();
        EasyMock.expect(taskManager.processId()).andReturn(processId).anyTimes();
        EasyMock.replay(taskManager);
    }

    @Test
    public void shouldInterleaveTasksByGroupId() {
        final TaskId taskIdA0 = new TaskId(0, 0);
        final TaskId taskIdA1 = new TaskId(0, 1);
        final TaskId taskIdA2 = new TaskId(0, 2);
        final TaskId taskIdA3 = new TaskId(0, 3);

        final TaskId taskIdB0 = new TaskId(1, 0);
        final TaskId taskIdB1 = new TaskId(1, 1);
        final TaskId taskIdB2 = new TaskId(1, 2);

        final TaskId taskIdC0 = new TaskId(2, 0);
        final TaskId taskIdC1 = new TaskId(2, 1);

        final List<TaskId> expectedSubList1 = Arrays.asList(taskIdA0, taskIdA3, taskIdB2);
        final List<TaskId> expectedSubList2 = Arrays.asList(taskIdA1, taskIdB0, taskIdC0);
        final List<TaskId> expectedSubList3 = Arrays.asList(taskIdA2, taskIdB1, taskIdC1);
        final List<List<TaskId>> embeddedList = Arrays.asList(expectedSubList1, expectedSubList2, expectedSubList3);

        List<TaskId> tasks = Arrays.asList(taskIdC0, taskIdC1, taskIdB0, taskIdB1, taskIdB2, taskIdA0, taskIdA1, taskIdA2, taskIdA3);
        Collections.shuffle(tasks);

        final List<List<TaskId>> interleavedTaskIds = partitionAssignor.interleaveTasksByGroupId(tasks, 3);

        assertThat(interleavedTaskIds, equalTo(embeddedList));
    }

    @Test
    public void testSubscription() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        final Set<TaskId> prevTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> cachedTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
                new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        final UUID processId = UUID.randomUUID();
        mockTaskManager(prevTasks, cachedTasks, processId, builder);

        configurePartitionAssignor(Collections.<String, Object>emptyMap());
        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("topic1", "topic2"));

        Collections.sort(subscription.topics());
        assertEquals(Utils.mkList("topic1", "topic2"), subscription.topics());

        Set<TaskId> standbyTasks = new HashSet<>(cachedTasks);
        standbyTasks.removeAll(prevTasks);

        SubscriptionInfo info = new SubscriptionInfo(processId, prevTasks, standbyTasks, null);
        assertEquals(info.encode(), subscription.userData());
    }

    @Test
    public void testAssignBasic() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
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

        mockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10, userEndPoint).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks11, standbyTasks11, userEndPoint).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks20, standbyTasks20, userEndPoint).encode()));


        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assigned partitions
        assertEquals(Utils.mkSet(Utils.mkSet(t1p0, t2p0), Utils.mkSet(t1p1, t2p1)),
                Utils.mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));
        assertEquals(Utils.mkSet(t1p2, t2p2), new HashSet<>(assignments.get("consumer20").partitions()));

        // check assignment info

        // the first consumer
        AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        // the second consumer
        AssignmentInfo info11 = checkAssignment(allTopics, assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks());

        assertEquals(Utils.mkSet(task0, task1), allActiveTasks);

        // the third consumer
        AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @Test
    public void shouldAssignEvenlyAcrossConsumersOneClientMultipleThreads() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1");
        builder.addProcessor("processorII", new MockProcessorSupplier(),  "source2");

        final List<PartitionInfo> localInfos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic1", 3, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo("topic2", 3, Node.noNode(), new Node[0], new Node[0])
        );

        final Cluster localMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            localInfos, Collections.<String>emptySet(),
            Collections.<String>emptySet());

        final List<String> topics = Utils.mkList("topic1", "topic2");

        final TaskId taskIdA0 = new TaskId(0, 0);
        final TaskId taskIdA1 = new TaskId(0, 1);
        final TaskId taskIdA2 = new TaskId(0, 2);
        final TaskId taskIdA3 = new TaskId(0, 3);

        final TaskId taskIdB0 = new TaskId(1, 0);
        final TaskId taskIdB1 = new TaskId(1, 1);
        final TaskId taskIdB2 = new TaskId(1, 2);
        final TaskId taskIdB3 = new TaskId(1, 3);

        final UUID uuid1 = UUID.randomUUID();

        mockTaskManager(new HashSet<TaskId>(), new HashSet<TaskId>(), uuid1, builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                          new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, new HashSet<TaskId>(), new HashSet<TaskId>(), userEndPoint).encode()));
        subscriptions.put("consumer11",
                          new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, new HashSet<TaskId>(), new HashSet<TaskId>(), userEndPoint).encode()));

        final Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(localMetadata, subscriptions);

        // check assigned partitions
        assertEquals(Utils.mkSet(Utils.mkSet(t2p2, t1p0, t1p2, t2p0), Utils.mkSet(t1p1, t2p1, t1p3, t2p3)),
                     Utils.mkSet(new HashSet<>(assignments.get("consumer10").partitions()), new HashSet<>(assignments.get("consumer11").partitions())));

        // the first consumer
        final AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());

        final List<TaskId> expectedInfo10TaskIds = Arrays.asList(taskIdA1, taskIdA3, taskIdB1, taskIdB3);
        assertEquals(expectedInfo10TaskIds, info10.activeTasks());

        // the second consumer
        final AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        final List<TaskId> expectedInfo11TaskIds = Arrays.asList(taskIdA0, taskIdA2, taskIdB0, taskIdB2);

        assertEquals(expectedInfo11TaskIds, info11.activeTasks());
    }

    @Test
    public void testAssignWithPartialTopology() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addStateStore(new MockStoreBuilder("store1", false), "processor1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        builder.addStateStore(new MockStoreBuilder("store2", false), "processor2");
        List<String> topics = Utils.mkList("topic1", "topic2");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        UUID uuid1 = UUID.randomUUID();

        mockTaskManager(Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), uuid1, builder);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, (Object) SingleGroupPartitionGrouperStub.class));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));
        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
            new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), userEndPoint).encode()));


        // will throw exception if it fails
        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assignment info
        AssignmentInfo info10 = checkAssignment(Utils.mkSet("topic1"), assignments.get("consumer10"));
        Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));
    }


    @Test
    public void testAssignEmptyMetadata() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        List<String> topics = Utils.mkList("topic1", "topic2");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        final Set<TaskId> prevTasks10 = Utils.mkSet(task0);
        final Set<TaskId> standbyTasks10 = Utils.mkSet(task1);
        final  Cluster emptyMetadata = new Cluster("cluster", Collections.singletonList(Node.noNode()),
            Collections.<PartitionInfo>emptySet(),
            Collections.<String>emptySet(),
            Collections.<String>emptySet());
        UUID uuid1 = UUID.randomUUID();

        mockTaskManager(prevTasks10, standbyTasks10, uuid1, builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
            new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10, userEndPoint).encode()));

        // initially metadata is empty
        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(emptyMetadata, subscriptions);

        // check assigned partitions
        assertEquals(Collections.<TopicPartition>emptySet(),
            new HashSet<>(assignments.get("consumer10").partitions()));

        // check assignment info
        AssignmentInfo info10 = checkAssignment(Collections.<String>emptySet(), assignments.get("consumer10"));
        Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());

        assertEquals(0, allActiveTasks.size());
        assertEquals(Collections.<TaskId>emptySet(), new HashSet<>(allActiveTasks));

        // then metadata gets populated
        assignments = partitionAssignor.assign(metadata, subscriptions);
        // check assigned partitions
        assertEquals(Utils.mkSet(Utils.mkSet(t1p0, t2p0, t1p0, t2p0, t1p1, t2p1, t1p2, t2p2)),
            Utils.mkSet(new HashSet<>(assignments.get("consumer10").partitions())));

        // the first consumer
        info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        allActiveTasks.addAll(info10.activeTasks());

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @Test
    public void testAssignWithNewTasks() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addSource(null, "source3", null, null, null, "topic3");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2", "source3");
        List<String> topics = Utils.mkList("topic1", "topic2", "topic3");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2, task3);

        // assuming that previous tasks do not have topic3
        final Set<TaskId> prevTasks10 = Utils.mkSet(task0);
        final Set<TaskId> prevTasks11 = Utils.mkSet(task1);
        final Set<TaskId> prevTasks20 = Utils.mkSet(task2);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        mockTaskManager(prevTasks10, Collections.<TaskId>emptySet(), uuid1, builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, Collections.<TaskId>emptySet(), userEndPoint).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks11, Collections.<TaskId>emptySet(), userEndPoint).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks20, Collections.<TaskId>emptySet(), userEndPoint).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assigned partitions: since there is no previous task for topic 3 it will be assigned randomly so we cannot check exact match
        // also note that previously assigned partitions / tasks may not stay on the previous host since we may assign the new task first and
        // then later ones will be re-assigned to other hosts due to load balancing
        AssignmentInfo info = AssignmentInfo.decode(assignments.get("consumer10").userData());
        Set<TaskId> allActiveTasks = new HashSet<>(info.activeTasks());
        Set<TopicPartition> allPartitions = new HashSet<>(assignments.get("consumer10").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer11").userData());
        allActiveTasks.addAll(info.activeTasks());
        allPartitions.addAll(assignments.get("consumer11").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer20").userData());
        allActiveTasks.addAll(info.activeTasks());
        allPartitions.addAll(assignments.get("consumer20").partitions());

        assertEquals(allTasks, allActiveTasks);
        assertEquals(Utils.mkSet(t1p0, t1p1, t1p2, t2p0, t2p1, t2p2, t3p0, t3p1, t3p2, t3p3), allPartitions);
    }

    @Test
    public void testAssignWithStates() {
        builder.setApplicationId(applicationId);
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source1");
        builder.addStateStore(new MockStoreBuilder("store1", false), "processor-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source2");
        builder.addStateStore(new MockStoreBuilder("store2", false), "processor-2");
        builder.addStateStore(new MockStoreBuilder("store3", false), "processor-2");

        List<String> topics = Utils.mkList("topic1", "topic2");

        TaskId task00 = new TaskId(0, 0);
        TaskId task01 = new TaskId(0, 1);
        TaskId task02 = new TaskId(0, 2);
        TaskId task10 = new TaskId(1, 0);
        TaskId task11 = new TaskId(1, 1);
        TaskId task12 = new TaskId(1, 2);
        List<TaskId> tasks = Utils.mkList(task00, task01, task02, task10, task11, task12);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        mockTaskManager(
            Collections.<TaskId>emptySet(),
            Collections.<TaskId>emptySet(),
            uuid1,
            builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), userEndPoint).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), userEndPoint).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), userEndPoint).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check assigned partition size: since there is no previous task and there are two sub-topologies the assignment is random so we cannot check exact match
        assertEquals(2, assignments.get("consumer10").partitions().size());
        assertEquals(2, assignments.get("consumer11").partitions().size());
        assertEquals(2, assignments.get("consumer20").partitions().size());

        AssignmentInfo info10 = AssignmentInfo.decode(assignments.get("consumer10").userData());
        AssignmentInfo info11 = AssignmentInfo.decode(assignments.get("consumer11").userData());
        AssignmentInfo info20 = AssignmentInfo.decode(assignments.get("consumer20").userData());

        assertEquals(2, info10.activeTasks().size());
        assertEquals(2, info11.activeTasks().size());
        assertEquals(2, info20.activeTasks().size());

        Set<TaskId> allTasks = new HashSet<>();
        allTasks.addAll(info10.activeTasks());
        allTasks.addAll(info11.activeTasks());
        allTasks.addAll(info20.activeTasks());
        assertEquals(new HashSet<>(tasks), allTasks);

        // check tasks for state topics
        Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        assertEquals(Utils.mkSet(task00, task01, task02), tasksForState(applicationId, "store1", tasks, topicGroups));
        assertEquals(Utils.mkSet(task10, task11, task12), tasksForState(applicationId, "store2", tasks, topicGroups));
        assertEquals(Utils.mkSet(task10, task11, task12), tasksForState(applicationId, "store3", tasks, topicGroups));
    }

    private Set<TaskId> tasksForState(final String applicationId,
                                      final String storeName,
                                      final List<TaskId> tasks,
                                      final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups) {
        final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);

        Set<TaskId> ids = new HashSet<>();
        for (Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            Set<String> stateChangelogTopics = entry.getValue().stateChangelogTopics.keySet();

            if (stateChangelogTopics.contains(changelogTopic)) {
                for (TaskId id : tasks) {
                    if (id.topicGroupId == entry.getKey())
                        ids.add(id);
                }
            }
        }
        return ids;
    }

    @Test
    public void testAssignWithStandbyReplicas() {
        Map<String, Object> props = configProps();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        StreamsConfig streamsConfig = new StreamsConfig(props);

        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");
        List<String> topics = Utils.mkList("topic1", "topic2");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);


        final Set<TaskId> prevTasks00 = Utils.mkSet(task0);
        final Set<TaskId> prevTasks01 = Utils.mkSet(task1);
        final Set<TaskId> prevTasks02 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks01 = Utils.mkSet(task1);
        final Set<TaskId> standbyTasks02 = Utils.mkSet(task2);
        final Set<TaskId> standbyTasks00 = Utils.mkSet(task0);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        mockTaskManager(prevTasks00, standbyTasks01, uuid1, builder);

        configurePartitionAssignor(Collections.<String, Object>singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks00, standbyTasks01, userEndPoint).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks01, standbyTasks02, userEndPoint).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks02, standbyTasks00, "any:9097").encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // the first consumer
        AssignmentInfo info10 = checkAssignment(allTopics, assignments.get("consumer10"));
        Set<TaskId> allActiveTasks = new HashSet<>(info10.activeTasks());
        Set<TaskId> allStandbyTasks = new HashSet<>(info10.standbyTasks().keySet());

        // the second consumer
        AssignmentInfo info11 = checkAssignment(allTopics, assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks());
        allStandbyTasks.addAll(info11.standbyTasks().keySet());

        assertNotEquals("same processId has same set of standby tasks", info11.standbyTasks().keySet(), info10.standbyTasks().keySet());

        // check active tasks assigned to the first client
        assertEquals(Utils.mkSet(task0, task1), new HashSet<>(allActiveTasks));
        assertEquals(Utils.mkSet(task2), new HashSet<>(allStandbyTasks));

        // the third consumer
        AssignmentInfo info20 = checkAssignment(allTopics, assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks());
        allStandbyTasks.addAll(info20.standbyTasks().keySet());

        // all task ids are in the active tasks and also in the standby tasks

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);

        assertEquals(3, allStandbyTasks.size());
        assertEquals(allTasks, allStandbyTasks);
    }

    @Test
    public void testOnAssignment() {
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        final List<TaskId> activeTaskList = Utils.mkList(task0, task3);
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
                new HostInfo("localhost", 9090),
                Utils.mkSet(t3p0, t3p3));
        activeTasks.put(task0, Utils.mkSet(t3p0));
        activeTasks.put(task3, Utils.mkSet(t3p3));
        standbyTasks.put(task1, Utils.mkSet(t3p1));
        standbyTasks.put(task2, Utils.mkSet(t3p2));

        final AssignmentInfo info = new AssignmentInfo(activeTaskList, standbyTasks, hostState);
        final PartitionAssignor.Assignment assignment = new PartitionAssignor.Assignment(Utils.mkList(t3p0, t3p3), info.encode());

        Capture<Cluster> capturedCluster = EasyMock.newCapture();
        taskManager.setPartitionsByHostState(hostState);
        EasyMock.expectLastCall();
        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
        EasyMock.expectLastCall();
        taskManager.setClusterMetadata(EasyMock.capture(capturedCluster));
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager);

        partitionAssignor.onAssignment(assignment);

        EasyMock.verify(taskManager);

        assertEquals(Collections.singleton(t3p0.topic()), capturedCluster.getValue().topics());
        assertEquals(2, capturedCluster.getValue().partitionsForTopic(t3p0.topic()).size());
    }

    @Test
    public void testAssignWithInternalTopics() {
        builder.setApplicationId(applicationId);
        builder.addInternalTopic("topicX");
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        List<String> topics = Utils.mkList("topic1", applicationId + "-topicX");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        UUID uuid1 = UUID.randomUUID();
        mockTaskManager(Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), uuid1, builder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());
        MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));

        partitionAssignor.assign(metadata, subscriptions);

        // check prepared internal topics
        assertEquals(1, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get(applicationId + "-topicX"));
    }

    @Test
    public void testAssignWithInternalTopicThatsSourceIsAnotherInternalTopic() {
        String applicationId = "test";
        builder.setApplicationId(applicationId);
        builder.addInternalTopic("topicX");
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", null, null, null, "processor1");
        builder.addSource(null, "source2", null, null, null, "topicX");
        builder.addInternalTopic("topicZ");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        builder.addSink("sink2", "topicZ", null, null, null, "processor2");
        builder.addSource(null, "source3", null, null, null, "topicZ");
        List<String> topics = Utils.mkList("topic1", "test-topicX", "test-topicZ");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        UUID uuid1 = UUID.randomUUID();
        mockTaskManager(Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), uuid1, builder);

        configurePartitionAssignor(Collections.<String, Object>emptyMap());
        MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));

        partitionAssignor.assign(metadata, subscriptions);

        // check prepared internal topics
        assertEquals(2, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get("test-topicZ"));
    }

    @Test
    public void shouldGenerateTasksForAllCreatedPartitions() {
        final StreamsBuilder builder = new StreamsBuilder();

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(applicationId);

        // KStream with 3 partitions
        KStream<Object, Object> stream1 = builder
            .stream("topic1")
            // force creation of internal repartition topic
            .map(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
                @Override
                public KeyValue<Object, Object> apply(final Object key, final Object value) {
                    return new KeyValue<>(key, value);
                }
            });

        // KTable with 4 partitions
        KTable<Object, Long> table1 = builder
            .table("topic3")
            // force creation of internal repartition topic
            .groupBy(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
                @Override
                public KeyValue<Object, Object> apply(final Object key, final Object value) {
                    return new KeyValue<>(key, value);
                }
            })
            .count();

        // joining the stream and the table
        // this triggers the enforceCopartitioning() routine in the StreamsPartitionAssignor,
        // forcing the stream.map to get repartitioned to a topic with four partitions.
        stream1.join(
            table1,
            new ValueJoiner() {
                @Override
                public Object apply(final Object value1, final Object value2) {
                    return null;
                }
            });

        final UUID uuid = UUID.randomUUID();
        final String client = "client1";

        mockTaskManager(
            Collections.<TaskId>emptySet(),
            Collections.<TaskId>emptySet(),
            UUID.randomUUID(),
            internalTopologyBuilder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put(
            client,
            new PartitionAssignor.Subscription(
                Utils.mkList("topic1", "topic3"),
                new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode()
            )
        );

        final Map<String, PartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, subscriptions);

        final Map<String, Integer> expectedCreatedInternalTopics = new HashMap<>();
        expectedCreatedInternalTopics.put(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 4);
        expectedCreatedInternalTopics.put(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-changelog", 4);
        expectedCreatedInternalTopics.put(applicationId + "-KSTREAM-MAP-0000000001-repartition", 4);

        // check if all internal topics were created as expected
        assertThat(mockInternalTopicManager.readyTopics, equalTo(expectedCreatedInternalTopics));

        final List<TopicPartition> expectedAssignment = Arrays.asList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic1", 2),
            new TopicPartition("topic3", 0),
            new TopicPartition("topic3", 1),
            new TopicPartition("topic3", 2),
            new TopicPartition("topic3", 3),
            new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 0),
            new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 1),
            new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 2),
            new TopicPartition(applicationId + "-KTABLE-AGGREGATE-STATE-STORE-0000000006-repartition", 3),
            new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 0),
            new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 1),
            new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 2),
            new TopicPartition(applicationId + "-KSTREAM-MAP-0000000001-repartition", 3)
        );

        // check if we created a task for all expected topicPartitions.
        assertThat(new HashSet<>(assignment.get(client).partitions()), equalTo(new HashSet<>(expectedAssignment)));
    }

    @Test
    public void shouldAddUserDefinedEndPointToSubscription() {
        builder.setApplicationId(applicationId);
        builder.addSource(null, "source", null, null, null, "input");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final UUID uuid1 = UUID.randomUUID();
        mockTaskManager(
            Collections.<TaskId>emptySet(),
            Collections.<TaskId>emptySet(),
            uuid1,
            builder);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, (Object) userEndPoint));
        final PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("input"));
        final SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
        assertEquals("localhost:8080", subscriptionInfo.userEndPoint());
    }

    @Test
    public void shouldMapUserEndPointToTopicPartitions() {
        builder.setApplicationId(applicationId);
        builder.addSource(null, "source", null, null, null, "topic1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", null, null, null, "processor");

        final List<String> topics = Utils.mkList("topic1");

        final UUID uuid1 = UUID.randomUUID();

        mockTaskManager(Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), uuid1, builder);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, (Object) userEndPoint));

        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put("consumer1",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));

        final Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);
        final PartitionAssignor.Assignment consumerAssignment = assignments.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumerAssignment.userData());
        final Set<TopicPartition> topicPartitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        assertEquals(
            Utils.mkSet(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)),
            topicPartitions);
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigIsNotHostPortPair() {
        builder.setApplicationId(applicationId);

        mockTaskManager(Collections.<TaskId>emptySet(), Collections.<TaskId>emptySet(), UUID.randomUUID(), builder);
        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(streamsConfig, mockClientSupplier.restoreConsumer));

        try {
            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, (Object) "localhost"));
            fail("expected to an exception due to invalid config");
        } catch (ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger() {
        builder.setApplicationId(applicationId);

        try {
            configurePartitionAssignor(Collections.singletonMap(StreamsConfig.APPLICATION_SERVER_CONFIG, (Object) "localhost:j87yhk"));
            fail("expected to an exception due to invalid config");
        } catch (ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldNotLoopInfinitelyOnMissingMetadataAndShouldNotCreateRelatedTasks() {
        final StreamsBuilder builder = new StreamsBuilder();

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(applicationId);

        KStream<Object, Object> stream1 = builder

            // Task 1 (should get created):
            .stream("topic1")
            // force repartitioning for aggregation
            .selectKey(new KeyValueMapper<Object, Object, Object>() {
                @Override
                public Object apply(final Object key, final Object value) {
                    return null;
                }
            })
            .groupByKey()

            // Task 2 (should get created):
            // create repartioning and changelog topic as task 1 exists
            .count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("count"))

            // force repartitioning for join, but second join input topic unknown
            // -> internal repartitioning topic should not get created
            .toStream()
            .map(new KeyValueMapper<Object, Long, KeyValue<Object, Object>>() {
                @Override
                public KeyValue<Object, Object> apply(final Object key, final Long value) {
                    return null;
                }
            });

        builder
            // Task 3 (should not get created because input topic unknown)
            .stream("unknownTopic")

            // force repartitioning for join, but input topic unknown
            // -> thus should not create internal repartitioning topic
            .selectKey(new KeyValueMapper<Object, Object, Object>() {
                @Override
                public Object apply(final Object key, final Object value) {
                    return null;
                }
            })

            // Task 4 (should not get created because input topics unknown)
            // should not create any of both input repartition topics or any of both changelog topics
            .join(
                stream1,
                new ValueJoiner() {
                    @Override
                    public Object apply(final Object value1, final Object value2) {
                        return null;
                    }
                },
                JoinWindows.of(0)
            );

        final UUID uuid = UUID.randomUUID();
        final String client = "client1";

        mockTaskManager(Collections.<TaskId>emptySet(),
                               Collections.<TaskId>emptySet(),
                               UUID.randomUUID(),
                internalTopologyBuilder);
        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put(
            client,
            new PartitionAssignor.Subscription(
                Collections.singletonList("unknownTopic"),
                new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode()
            )
        );

        final Map<String, PartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, subscriptions);

        final Map<String, Integer> expectedCreatedInternalTopics = new HashMap<>();
        expectedCreatedInternalTopics.put(applicationId + "-count-repartition", 3);
        expectedCreatedInternalTopics.put(applicationId + "-count-changelog", 3);
        assertThat(mockInternalTopicManager.readyTopics, equalTo(expectedCreatedInternalTopics));

        final List<TopicPartition> expectedAssignment = Arrays.asList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic1", 2),
            new TopicPartition(applicationId + "-count-repartition", 0),
            new TopicPartition(applicationId + "-count-repartition", 1),
            new TopicPartition(applicationId + "-count-repartition", 2)
        );
        assertThat(new HashSet<>(assignment.get(client).partitions()), equalTo(new HashSet<>(expectedAssignment)));
    }

    @Test
    public void shouldUpdateClusterMetadataAndHostInfoOnAssignment() {
        final TopicPartition partitionOne = new TopicPartition("topic", 1);
        final TopicPartition partitionTwo = new TopicPartition("topic", 2);
        final Map<HostInfo, Set<TopicPartition>> hostState = Collections.singletonMap(
                new HostInfo("localhost", 9090), Utils.mkSet(partitionOne, partitionTwo));

        configurePartitionAssignor(Collections.<String, Object>emptyMap());

        taskManager.setPartitionsByHostState(hostState);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager);

        partitionAssignor.onAssignment(createAssignment(hostState));

        EasyMock.verify(taskManager);
    }

    @Test
    public void shouldNotAddStandbyTaskPartitionsToPartitionsForHost() {
        final StreamsBuilder builder = new StreamsBuilder();

        final InternalTopologyBuilder internalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.build());
        internalTopologyBuilder.setApplicationId(applicationId);

        builder.stream("topic1").groupByKey().count();

        final UUID uuid = UUID.randomUUID();
        mockTaskManager(
            Collections.<TaskId>emptySet(),
            Collections.<TaskId>emptySet(),
            uuid,
            internalTopologyBuilder);

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint);
        configurePartitionAssignor(props);
        partitionAssignor.setInternalTopicManager(new MockInternalTopicManager(
            streamsConfig,
            mockClientSupplier.restoreConsumer));

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put(
                "consumer1",
                new PartitionAssignor.Subscription(
                        Collections.singletonList("topic1"),
                        new SubscriptionInfo(uuid, emptyTasks, emptyTasks, userEndPoint).encode()
                )
        );

        subscriptions.put(
                "consumer2",
                new PartitionAssignor.Subscription(
                        Collections.singletonList("topic1"),
                        new SubscriptionInfo(UUID.randomUUID(), emptyTasks, emptyTasks, "other:9090").encode()
                )
        );
        final Set<TopicPartition> allPartitions = Utils.mkSet(t1p0, t1p1, t1p2);
        final Map<String, PartitionAssignor.Assignment> assign = partitionAssignor.assign(metadata, subscriptions);
        final PartitionAssignor.Assignment consumer1Assignment = assign.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumer1Assignment.userData());
        final Set<TopicPartition> consumer1partitions = assignmentInfo.partitionsByHost().get(new HostInfo("localhost", 8080));
        final Set<TopicPartition> consumer2Partitions = assignmentInfo.partitionsByHost().get(new HostInfo("other", 9090));
        final HashSet<TopicPartition> allAssignedPartitions = new HashSet<>(consumer1partitions);
        allAssignedPartitions.addAll(consumer2Partitions);
        assertThat(consumer1partitions, not(allPartitions));
        assertThat(consumer2Partitions, not(allPartitions));
        assertThat(allAssignedPartitions, equalTo(allPartitions));
    }

    @Test(expected = KafkaException.class)
    public void shouldThrowKafkaExceptionIfStreamThreadNotConfigured() {
        partitionAssignor.configure(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));
    }

    @Test(expected = KafkaException.class)
    public void shouldThrowKafkaExceptionIfStreamThreadConfigIsNotThreadDataProviderInstance() {
        final Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, "i am not a stream thread");

        partitionAssignor.configure(config);
    }

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V2() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 2);
    }

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV1V3() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(1, 3);
    }

    @Test
    public void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersionsV2V3() {
        shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(2, 3);
    }

    private void shouldReturnLowestAssignmentVersionForDifferentSubscriptionVersions(final int smallestVersion,
                                                                                     final int otherVersion) {
        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.emptySet();
        subscriptions.put(
            "consumer1",
            new PartitionAssignor.Subscription(
                Collections.singletonList("topic1"),
                new SubscriptionInfo(smallestVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode()
            )
        );
        subscriptions.put(
            "consumer2",
            new PartitionAssignor.Subscription(
                Collections.singletonList("topic1"),
                new SubscriptionInfo(otherVersion, UUID.randomUUID(), emptyTasks, emptyTasks, null).encode()
            )
        );

        mockTaskManager(
            emptyTasks,
            emptyTasks,
            UUID.randomUUID(),
            builder);
        partitionAssignor.configure(configProps());
        final Map<String, PartitionAssignor.Assignment> assignment = partitionAssignor.assign(metadata, subscriptions);

        assertThat(assignment.size(), equalTo(2));
        assertThat(AssignmentInfo.decode(assignment.get("consumer1").userData()).version(), equalTo(smallestVersion));
        assertThat(AssignmentInfo.decode(assignment.get("consumer2").userData()).version(), equalTo(smallestVersion));
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion1() {
        final Set<TaskId> emptyTasks = Collections.emptySet();

        mockTaskManager(
            emptyTasks,
            emptyTasks,
            UUID.randomUUID(),
            builder);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, (Object) StreamsConfig.UPGRADE_FROM_0100));

        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("topic1"));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(1));
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0101() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0101);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0102() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0102);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For0110() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_0110);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For10() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_10);
    }

    @Test
    public void shouldDownGradeSubscriptionToVersion2For11() {
        shouldDownGradeSubscriptionToVersion2(StreamsConfig.UPGRADE_FROM_11);
    }

    private void shouldDownGradeSubscriptionToVersion2(final Object upgradeFromValue) {
        final Set<TaskId> emptyTasks = Collections.emptySet();

        mockTaskManager(
            emptyTasks,
            emptyTasks,
            UUID.randomUUID(),
            builder);
        configurePartitionAssignor(Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFromValue));

        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("topic1"));

        assertThat(SubscriptionInfo.decode(subscription.userData()).version(), equalTo(2));
    }

    private PartitionAssignor.Assignment createAssignment(final Map<HostInfo, Set<TopicPartition>> firstHostState) {
        final AssignmentInfo info = new AssignmentInfo(Collections.<TaskId>emptyList(),
                                                       Collections.<TaskId, Set<TopicPartition>>emptyMap(),
                                                       firstHostState);

        return new PartitionAssignor.Assignment(
                Collections.<TopicPartition>emptyList(), info.encode());
    }

    private AssignmentInfo checkAssignment(Set<String> expectedTopics, PartitionAssignor.Assignment assignment) {

        // This assumed 1) DefaultPartitionGrouper is used, and 2) there is an only one topic group.

        AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

        // check if the number of assigned partitions == the size of active task id list
        assertEquals(assignment.partitions().size(), info.activeTasks().size());

        // check if active tasks are consistent
        List<TaskId> activeTasks = new ArrayList<>();
        Set<String> activeTopics = new HashSet<>();
        for (TopicPartition partition : assignment.partitions()) {
            // since default grouper, taskid.partition == partition.partition()
            activeTasks.add(new TaskId(0, partition.partition()));
            activeTopics.add(partition.topic());
        }
        assertEquals(activeTasks, info.activeTasks());

        // check if active partitions cover all topics
        assertEquals(expectedTopics, activeTopics);

        // check if standby tasks are consistent
        Set<String> standbyTopics = new HashSet<>();
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : info.standbyTasks().entrySet()) {
            TaskId id = entry.getKey();
            Set<TopicPartition> partitions = entry.getValue();
            for (TopicPartition partition : partitions) {
                // since default grouper, taskid.partition == partition.partition()
                assertEquals(id.partition, partition.partition());

                standbyTopics.add(partition.topic());
            }
        }

        if (info.standbyTasks().size() > 0) {
            // check if standby partitions cover all topics
            assertEquals(expectedTopics, standbyTopics);
        }

        return info;
    }
}
