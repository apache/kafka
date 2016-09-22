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
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Assert;
import org.junit.Test;

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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class StreamPartitionAssignorTest {

    private TopicPartition t1p0 = new TopicPartition("topic1", 0);
    private TopicPartition t1p1 = new TopicPartition("topic1", 1);
    private TopicPartition t1p2 = new TopicPartition("topic1", 2);
    private TopicPartition t2p0 = new TopicPartition("topic2", 0);
    private TopicPartition t2p1 = new TopicPartition("topic2", 1);
    private TopicPartition t2p2 = new TopicPartition("topic2", 2);
    private TopicPartition t3p0 = new TopicPartition("topic3", 0);
    private TopicPartition t3p1 = new TopicPartition("topic3", 1);
    private TopicPartition t3p2 = new TopicPartition("topic3", 2);
    private TopicPartition t3p3 = new TopicPartition("topic3", 3);

    private Set<String> allTopics = Utils.mkSet("topic1", "topic2");

    private List<PartitionInfo> infos = Arrays.asList(
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

    private Cluster metadata = new Cluster("cluster", Arrays.asList(Node.noNode()), infos, Collections.<String>emptySet(),
        Collections.<String>emptySet());

    private final TaskId task0 = new TaskId(0, 0);
    private final TaskId task1 = new TaskId(0, 1);
    private final TaskId task2 = new TaskId(0, 2);
    private final TaskId task3 = new TaskId(0, 3);
    private String userEndPoint = null;

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-partition-assignor-test");
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSubscription() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        final Set<TaskId> prevTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1));
        final Set<TaskId> cachedTasks = Utils.mkSet(
                new TaskId(0, 1), new TaskId(1, 1), new TaskId(2, 1),
                new TaskId(0, 2), new TaskId(1, 2), new TaskId(2, 2));

        String clientId = "client-id";
        UUID processId = UUID.randomUUID();
        StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), "test", clientId, processId, new Metrics(), new SystemTime(), new StreamsMetadataState(builder)) {
            @Override
            public Set<TaskId> prevTasks() {
                return prevTasks;
            }
            @Override
            public Set<TaskId> cachedTasks() {
                return cachedTasks;
            }
        };

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread, "test", clientId));

        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("topic1", "topic2"));

        Collections.sort(subscription.topics());
        assertEquals(Utils.mkList("topic1", "topic2"), subscription.topics());

        Set<TaskId> standbyTasks = new HashSet<>(cachedTasks);
        standbyTasks.removeAll(prevTasks);

        SubscriptionInfo info = new SubscriptionInfo(processId, prevTasks, standbyTasks, null);
        assertEquals(info.encode(), subscription.userData());
    }

    @Test
    public void testAssignBasic() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());

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
        String client1 = "client1";

        StreamThread thread10 = new StreamThread(builder, config, new MockClientSupplier(), "test", client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, "test", client1));

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

        Set<TaskId> allActiveTasks = new HashSet<>();

        // the first consumer
        AssignmentInfo info10 = checkAssignment(assignments.get("consumer10"));
        allActiveTasks.addAll(info10.activeTasks);

        // the second consumer
        AssignmentInfo info11 = checkAssignment(assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks);

        assertEquals(Utils.mkSet(task0, task1), allActiveTasks);

        // the third consumer
        AssignmentInfo info20 = checkAssignment(assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks);

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, new HashSet<>(allActiveTasks));

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);
    }

    @Test
    public void testAssignWithNewTasks() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addSource("source3", "topic3");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2", "source3");
        List<String> topics = Utils.mkList("topic1", "topic2", "topic3");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2, task3);

        // assuming that previous tasks do not have topic3
        final Set<TaskId> prevTasks10 = Utils.mkSet(task0);
        final Set<TaskId> prevTasks11 = Utils.mkSet(task1);
        final Set<TaskId> prevTasks20 = Utils.mkSet(task2);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        String client1 = "client1";

        StreamThread thread10 = new StreamThread(builder, config, new MockClientSupplier(), "test", client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, "test", client1));

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
        Set<TaskId> allActiveTasks = new HashSet<>();
        Set<TopicPartition> allPartitions = new HashSet<>();
        AssignmentInfo info;

        info = AssignmentInfo.decode(assignments.get("consumer10").userData());
        allActiveTasks.addAll(info.activeTasks);
        allPartitions.addAll(assignments.get("consumer10").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer11").userData());
        allActiveTasks.addAll(info.activeTasks);
        allPartitions.addAll(assignments.get("consumer11").partitions());

        info = AssignmentInfo.decode(assignments.get("consumer20").userData());
        allActiveTasks.addAll(info.activeTasks);
        allPartitions.addAll(assignments.get("consumer20").partitions());

        assertEquals(allTasks, allActiveTasks);
        assertEquals(Utils.mkSet(t1p0, t1p1, t1p2, t2p0, t2p1, t2p2, t3p0, t3p1, t3p2, t3p3), allPartitions);
    }

    @Test
    public void testAssignWithStates() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());
        String applicationId = "test";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId(applicationId);
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source1");
        builder.addStateStore(new MockStateStoreSupplier("store1", false), "processor-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source2");
        builder.addStateStore(new MockStateStoreSupplier("store2", false), "processor-2");
        builder.addStateStore(new MockStateStoreSupplier("store3", false), "processor-2");

        List<String> topics = Utils.mkList("topic1", "topic2");

        TaskId task00 = new TaskId(0, 0);
        TaskId task01 = new TaskId(0, 1);
        TaskId task02 = new TaskId(0, 2);
        TaskId task10 = new TaskId(1, 0);
        TaskId task11 = new TaskId(1, 1);
        TaskId task12 = new TaskId(1, 2);

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        String client1 = "client1";


        StreamThread thread10 = new StreamThread(builder, config, new MockClientSupplier(), applicationId, client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, applicationId, client1));

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

        assertEquals(2, AssignmentInfo.decode(assignments.get("consumer10").userData()).activeTasks.size());
        assertEquals(2, AssignmentInfo.decode(assignments.get("consumer11").userData()).activeTasks.size());
        assertEquals(2, AssignmentInfo.decode(assignments.get("consumer20").userData()).activeTasks.size());

        // check tasks for state topics
        assertEquals(Utils.mkSet(task00, task01, task02), partitionAssignor.tasksForState("store1"));
        assertEquals(Utils.mkSet(task10, task11, task12), partitionAssignor.tasksForState("store2"));
        assertEquals(Utils.mkSet(task10, task11, task12), partitionAssignor.tasksForState("store3"));
    }

    @Test
    public void testAssignWithStandbyReplicas() throws Exception {
        Properties props = configProps();
        props.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        StreamsConfig config = new StreamsConfig(props);

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
        String client1 = "client1";

        StreamThread thread10 = new StreamThread(builder, config, new MockClientSupplier(), "test", client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, "test", client1));

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks10, standbyTasks10, userEndPoint).encode()));
        subscriptions.put("consumer11",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, prevTasks11, standbyTasks11, userEndPoint).encode()));
        subscriptions.put("consumer20",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid2, prevTasks20, standbyTasks20, userEndPoint).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        Set<TaskId> allActiveTasks = new HashSet<>();
        Set<TaskId> allStandbyTasks = new HashSet<>();

        // the first consumer
        AssignmentInfo info10 = checkAssignment(assignments.get("consumer10"));
        allActiveTasks.addAll(info10.activeTasks);
        allStandbyTasks.addAll(info10.standbyTasks.keySet());

        // the second consumer
        AssignmentInfo info11 = checkAssignment(assignments.get("consumer11"));
        allActiveTasks.addAll(info11.activeTasks);
        allStandbyTasks.addAll(info11.standbyTasks.keySet());

        assertNotEquals("same processId has same set of standby tasks", info11.standbyTasks.keySet(), info10.standbyTasks.keySet());

        // check active tasks assigned to the first client
        assertEquals(Utils.mkSet(task0, task1), new HashSet<>(allActiveTasks));
        assertEquals(Utils.mkSet(task2), new HashSet<>(allStandbyTasks));

        // the third consumer
        AssignmentInfo info20 = checkAssignment(assignments.get("consumer20"));
        allActiveTasks.addAll(info20.activeTasks);
        allStandbyTasks.addAll(info20.standbyTasks.keySet());

        // all task ids are in the active tasks and also in the standby tasks

        assertEquals(3, allActiveTasks.size());
        assertEquals(allTasks, allActiveTasks);

        assertEquals(3, allStandbyTasks.size());
        assertEquals(allTasks, allStandbyTasks);
    }

    private AssignmentInfo checkAssignment(PartitionAssignor.Assignment assignment) {

        // This assumed 1) DefaultPartitionGrouper is used, and 2) there is a only one topic group.

        AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

        // check if the number of assigned partitions == the size of active task id list
        assertEquals(assignment.partitions().size(), info.activeTasks.size());

        // check if active tasks are consistent
        List<TaskId> activeTasks = new ArrayList<>();
        Set<String> activeTopics = new HashSet<>();
        for (TopicPartition partition : assignment.partitions()) {
            // since default grouper, taskid.partition == partition.partition()
            activeTasks.add(new TaskId(0, partition.partition()));
            activeTopics.add(partition.topic());
        }
        assertEquals(activeTasks, info.activeTasks);

        // check if active partitions cover all topics
        assertEquals(allTopics, activeTopics);

        // check if standby tasks are consistent
        Set<String> standbyTopics = new HashSet<>();
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : info.standbyTasks.entrySet()) {
            TaskId id = entry.getKey();
            Set<TopicPartition> partitions = entry.getValue();
            for (TopicPartition partition : partitions) {
                // since default grouper, taskid.partition == partition.partition()
                assertEquals(id.partition, partition.partition());

                standbyTopics.add(partition.topic());
            }
        }

        if (info.standbyTasks.size() > 0)
            // check if standby partitions cover all topics
            assertEquals(allTopics, standbyTopics);

        return info;
    }

    @Test
    public void testOnAssignment() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());

        TopicPartition t2p3 = new TopicPartition("topic2", 3);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source1", "topic1");
        builder.addSource("source2", "topic2");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source1", "source2");

        UUID uuid = UUID.randomUUID();
        String client1 = "client1";

        StreamThread thread = new StreamThread(builder, config, new MockClientSupplier(), "test", client1, uuid, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread, "test", client1));

        List<TaskId> activeTaskList = Utils.mkList(task0, task3);
        Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
        standbyTasks.put(task1, Utils.mkSet(new TopicPartition("t1", 0)));
        standbyTasks.put(task2, Utils.mkSet(new TopicPartition("t2", 0)));

        AssignmentInfo info = new AssignmentInfo(activeTaskList, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());
        PartitionAssignor.Assignment assignment = new PartitionAssignor.Assignment(Utils.mkList(t1p0, t2p3), info.encode());
        partitionAssignor.onAssignment(assignment);

        assertEquals(Utils.mkSet(task0), partitionAssignor.tasksForPartition(t1p0));
        assertEquals(Utils.mkSet(task3), partitionAssignor.tasksForPartition(t2p3));
        assertEquals(standbyTasks, partitionAssignor.standbyTasks());
    }

    @Test
    public void testAssignWithInternalTopics() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());
        String applicationId = "test";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId(applicationId);
        builder.addInternalTopic("topicX");
        builder.addSource("source1", "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", "processor1");
        builder.addSource("source2", "topicX");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        List<String> topics = Utils.mkList("topic1", "test-topicX");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        UUID uuid1 = UUID.randomUUID();
        String client1 = "client1";

        MockClientSupplier clientSupplier = new MockClientSupplier();

        StreamThread thread10 = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, applicationId, client1));
        MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(clientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        Set<TaskId> emptyTasks = Collections.<TaskId>emptySet();
        subscriptions.put("consumer10",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));

        partitionAssignor.assign(metadata, subscriptions);

        // check prepared internal topics
        assertEquals(1, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get("test-topicX"));
    }

    @Test
    public void testAssignWithInternalTopicThatsSourceIsAnotherInternalTopic() throws Exception {
        StreamsConfig config = new StreamsConfig(configProps());
        String applicationId = "test";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setApplicationId(applicationId);
        builder.addInternalTopic("topicX");
        builder.addSource("source1", "topic1");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addSink("sink1", "topicX", "processor1");
        builder.addSource("source2", "topicX");
        builder.addInternalTopic("topicZ");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source2");
        builder.addSink("sink2", "topicZ", "processor2");
        builder.addSource("source3", "topicZ");
        List<String> topics = Utils.mkList("topic1", "test-topicX", "test-topicZ");
        Set<TaskId> allTasks = Utils.mkSet(task0, task1, task2);

        UUID uuid1 = UUID.randomUUID();
        String client1 = "client1";

        MockClientSupplier clientSupplier = new MockClientSupplier();

        StreamThread thread10 = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(thread10, applicationId, client1));
        MockInternalTopicManager internalTopicManager = new MockInternalTopicManager(clientSupplier.restoreConsumer);
        partitionAssignor.setInternalTopicManager(internalTopicManager);

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        Set<TaskId> emptyTasks = Collections.<TaskId>emptySet();
        subscriptions.put("consumer10",
                          new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, userEndPoint).encode()));

        Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);

        // check prepared internal topics
        assertEquals(2, internalTopicManager.readyTopics.size());
        assertEquals(allTasks.size(), (long) internalTopicManager.readyTopics.get("test-topicZ"));
    }

    @Test
    public void shouldAddUserDefinedEndPointToSubscription() throws Exception {
        final Properties properties = configProps();
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");
        final StreamsConfig config = new StreamsConfig(properties);
        final TopologyBuilder builder = new TopologyBuilder();
        final String applicationId = "application-id";
        builder.setApplicationId(applicationId);
        builder.addSource("source", "input");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", "processor");

        final UUID uuid1 = UUID.randomUUID();
        final String client1 = "client1";

        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread streamThread = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(streamThread, applicationId, client1));
        final PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Utils.mkSet("input"));
        final SubscriptionInfo subscriptionInfo = SubscriptionInfo.decode(subscription.userData());
        assertEquals("localhost:8080", subscriptionInfo.userEndPoint);
    }

    @Test
    public void shouldMapUserEndPointToTopicPartitions() throws Exception {
        final Properties properties = configProps();
        final String myEndPoint = "localhost:8080";
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, myEndPoint);
        final StreamsConfig config = new StreamsConfig(properties);
        final TopologyBuilder builder = new TopologyBuilder();
        final String applicationId = "application-id";
        builder.setApplicationId(applicationId);
        builder.addSource("source", "topic1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addSink("sink", "output", "processor");

        final List<String> topics = Utils.mkList("topic1");

        final UUID uuid1 = UUID.randomUUID();
        final String client1 = "client1";

        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread streamThread = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1, new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        partitionAssignor.configure(config.getConsumerConfigs(streamThread, applicationId, client1));

        final Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        final Set<TaskId> emptyTasks = Collections.<TaskId>emptySet();
        subscriptions.put("consumer1",
                new PartitionAssignor.Subscription(topics, new SubscriptionInfo(uuid1, emptyTasks, emptyTasks, myEndPoint).encode()));

        final Map<String, PartitionAssignor.Assignment> assignments = partitionAssignor.assign(metadata, subscriptions);
        final PartitionAssignor.Assignment consumerAssignment = assignments.get("consumer1");
        final AssignmentInfo assignmentInfo = AssignmentInfo.decode(consumerAssignment.userData());
        final Set<TopicPartition> topicPartitions = assignmentInfo.partitionsByHostState.get(new HostInfo("localhost", 8080));
        assertEquals(Utils.mkSet(new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)), topicPartitions);
    }

    @Test
    public void shouldThrowExceptionIfApplicationServerConfigIsNotHostPortPair() throws Exception {
        final Properties properties = configProps();
        final String myEndPoint = "localhost";
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, myEndPoint);
        final StreamsConfig config = new StreamsConfig(properties);
        final UUID uuid1 = UUID.randomUUID();
        final String client1 = "client1";
        final TopologyBuilder builder = new TopologyBuilder();
        final String applicationId = "application-id";
        builder.setApplicationId(applicationId);

        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread streamThread = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1,
                                                           new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        try {
            partitionAssignor.configure(config.getConsumerConfigs(streamThread, applicationId, client1));
            Assert.fail("expected to an exception due to invalid config");
        } catch (ConfigException e) {
            // pass
        }
    }
    
    @Test
    public void shouldThrowExceptionIfApplicationServerConfigPortIsNotAnInteger() throws Exception {
        final Properties properties = configProps();
        final String myEndPoint = "localhost:j87yhk";
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, myEndPoint);
        final StreamsConfig config = new StreamsConfig(properties);
        final UUID uuid1 = UUID.randomUUID();
        final String client1 = "client1";
        final TopologyBuilder builder = new TopologyBuilder();
        final String applicationId = "application-id";
        builder.setApplicationId(applicationId);

        final MockClientSupplier clientSupplier = new MockClientSupplier();

        final StreamThread streamThread = new StreamThread(builder, config, clientSupplier, applicationId, client1, uuid1,
                                                           new Metrics(), new SystemTime(), new StreamsMetadataState(builder));

        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        try {
            partitionAssignor.configure(config.getConsumerConfigs(streamThread, applicationId, client1));
            Assert.fail("expected to an exception due to invalid config");
        } catch (ConfigException e) {
            // pass
        }
    }

    @Test
    public void shouldExposeHostStateToTopicPartitionsOnAssignment() throws Exception {
        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        List<TopicPartition> topic = Arrays.asList(new TopicPartition("topic", 0));
        final Map<HostInfo, Set<TopicPartition>> hostState =
                Collections.singletonMap(new HostInfo("localhost", 80),
                        Collections.singleton(new TopicPartition("topic", 0)));
        AssignmentInfo assignmentInfo = new AssignmentInfo(Collections.singletonList(new TaskId(0, 0)),
                Collections.<TaskId, Set<TopicPartition>>emptyMap(),
                hostState);
        partitionAssignor.onAssignment(new PartitionAssignor.Assignment(topic, assignmentInfo.encode()));
        assertEquals(hostState, partitionAssignor.getPartitionsByHostState());
    }

    @Test
    public void shouldSetClusterMetadataOnAssignment() throws Exception {
        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();

        final List<TopicPartition> topic = Arrays.asList(new TopicPartition("topic", 0));
        final Map<HostInfo, Set<TopicPartition>> hostState =
                Collections.singletonMap(new HostInfo("localhost", 80),
                                         Collections.singleton(new TopicPartition("topic", 0)));
        final AssignmentInfo assignmentInfo = new AssignmentInfo(Collections.singletonList(new TaskId(0, 0)),
                                                                 Collections.<TaskId, Set<TopicPartition>>emptyMap(),
                                                                 hostState);


        partitionAssignor.onAssignment(new PartitionAssignor.Assignment(topic, assignmentInfo.encode()));
        final Cluster cluster = partitionAssignor.clusterMetadata();
        final List<PartitionInfo> partitionInfos = cluster.partitionsForTopic("topic");
        final PartitionInfo partitionInfo = partitionInfos.get(0);
        assertEquals(1, partitionInfos.size());
        assertEquals("topic", partitionInfo.topic());
        assertEquals(0, partitionInfo.partition());
    }

    @Test
    public void shouldReturnEmptyClusterMetadataIfItHasntBeenBuilt() throws Exception {
        final StreamPartitionAssignor partitionAssignor = new StreamPartitionAssignor();
        final Cluster cluster = partitionAssignor.clusterMetadata();
        assertNotNull(cluster);

    }


    private class MockInternalTopicManager extends InternalTopicManager {

        public Map<String, Integer> readyTopics = new HashMap<>();
        public MockConsumer<byte[], byte[]> restoreConsumer;

        public MockInternalTopicManager(MockConsumer<byte[], byte[]> restoreConsumer) {
            super();

            this.restoreConsumer = restoreConsumer;
        }

        @Override
        public void makeReady(InternalTopicConfig topic, int numPartitions) {
            readyTopics.put(topic.name(), numPartitions);

            List<PartitionInfo> partitions = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new PartitionInfo(topic.name(), i, null, null, null));
            }

            restoreConsumer.updatePartitions(topic.name(), partitions);
        }
    }
}
