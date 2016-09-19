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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.processor.internals.InternalTopicManager.WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;

public class StreamPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(StreamPartitionAssignor.class);
    private String userEndPointConfig;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHostState;
    private Cluster metadataWithInternalTopics;


    private static class AssignedPartition implements Comparable<AssignedPartition> {
        public final TaskId taskId;
        public final TopicPartition partition;

        public AssignedPartition(TaskId taskId, TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }
    }

    private static final Comparator<TopicPartition> PARTITION_COMPARATOR = new Comparator<TopicPartition>() {
        @Override
        public int compare(TopicPartition p1, TopicPartition p2) {
            int result = p1.topic().compareTo(p2.topic());

            if (result != 0) {
                return result;
            } else {
                return p1.partition() < p2.partition() ? -1 : (p1.partition() > p2.partition() ? 1 : 0);
            }
        }
    };

    private StreamThread streamThread;

    private int numStandbyReplicas;
    private Map<Integer, TopologyBuilder.TopicsInfo> topicGroups;
    private Map<TopicPartition, Set<TaskId>> partitionToTaskIds;
    private Map<InternalTopicConfig, Set<TaskId>> stateChangelogTopicToTaskIds;
    private Map<InternalTopicConfig, Set<TaskId>> internalSourceTopicToTaskIds;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;

    private InternalTopicManager internalTopicManager;

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(Map<String, ?> configs) {
        numStandbyReplicas = (Integer) configs.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);


        Object o = configs.get(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE);
        if (o == null) {
            KafkaException ex = new KafkaException("StreamThread is not specified");
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        if (!(o instanceof StreamThread)) {
            KafkaException ex = new KafkaException(String.format("%s is not an instance of %s", o.getClass().getName(), StreamThread.class.getName()));
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        streamThread = (StreamThread) o;
        streamThread.partitionAssignor(this);

        String userEndPoint = (String) configs.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
        if (userEndPoint != null && !userEndPoint.isEmpty()) {
            final String[] hostPort = userEndPoint.split(":");
            if (hostPort.length != 2) {
                throw new ConfigException(String.format("stream-thread [%s] Config %s isn't in the correct format. Expected a host:port pair" +
                                                       " but received %s",
                        streamThread.getName(), StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
            } else {
                try {
                    Integer.valueOf(hostPort[1]);
                    this.userEndPointConfig = userEndPoint;
                } catch (NumberFormatException nfe) {
                    throw new ConfigException(String.format("stream-thread [%s] Invalid port %s supplied in %s for config %s",
                            streamThread.getName(), hostPort[1], userEndPoint, StreamsConfig.APPLICATION_SERVER_CONFIG));
                }
            }

        }

        if (configs.containsKey(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG)) {
            internalTopicManager = new InternalTopicManager(
                    (String) configs.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG),
                    configs.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG) ? (Integer) configs.get(StreamsConfig.REPLICATION_FACTOR_CONFIG) : 1,
                    configs.containsKey(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG) ?
                            (Long) configs.get(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG)
                            : WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT);
        } else {
            log.info("stream-thread [{}] Config '{}' isn't supplied and hence no internal topics will be created.",  streamThread.getName(), StreamsConfig.ZOOKEEPER_CONNECT_CONFIG);
        }
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        Set<TaskId> prevTasks = streamThread.prevTasks();
        Set<TaskId> standbyTasks = streamThread.cachedTasks();
        standbyTasks.removeAll(prevTasks);
        SubscriptionInfo data = new SubscriptionInfo(streamThread.processId, prevTasks, standbyTasks, this.userEndPointConfig);

        if (streamThread.builder.sourceTopicPattern() != null) {
            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            log.debug("have {} topics matching regex", topics);
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.updateTopics(topics);
            streamThread.builder.updateSubscriptions(subscriptionUpdates);
        }

        return new Subscription(new ArrayList<>(topics), data.encode());
    }

    /**
     * Internal helper function that creates a Kafka topic
     * @param topicToTaskIds Map that contains the topic names to be created
     * @param postPartitionPhase If true, the computation for calculating the number of partitions
     *                           is slightly different. Set to true after the initial topic-to-partition
     *                           assignment.
     * @return
     */
    private Map<TopicPartition, PartitionInfo> prepareTopic(Map<InternalTopicConfig, Set<TaskId>> topicToTaskIds,
                                                            boolean postPartitionPhase) {
        Map<TopicPartition, PartitionInfo> partitionInfos = new HashMap<>();
        // if ZK is specified, prepare the internal source topic before calling partition grouper
        if (internalTopicManager != null) {
            log.debug("stream-thread [{}] Starting to validate internal topics in partition assignor.", streamThread.getName());

            for (Map.Entry<InternalTopicConfig, Set<TaskId>> entry : topicToTaskIds.entrySet()) {
                InternalTopicConfig topic = entry.getKey();
                int numPartitions = 0;
                if (postPartitionPhase) {
                    // the expected number of partitions is the max value of TaskId.partition + 1
                    for (TaskId task : entry.getValue()) {
                        if (numPartitions < task.partition + 1)
                            numPartitions = task.partition + 1;
                    }
                } else {
                    // should have size 1 only
                    numPartitions = -1;
                    for (TaskId task : entry.getValue()) {
                        numPartitions = task.partition;
                    }
                }

                internalTopicManager.makeReady(topic, numPartitions);

                // wait until the topic metadata has been propagated to all brokers
                List<PartitionInfo> partitions;
                do {
                    partitions = streamThread.restoreConsumer.partitionsFor(topic.name());
                } while (partitions == null || partitions.size() != numPartitions);

                for (PartitionInfo partition : partitions)
                    partitionInfos.put(new TopicPartition(partition.topic(), partition.partition()), partition);
            }

            log.info("stream-thread [{}] Completed validating internal topics in partition assignor", streamThread.getName());
        } else {
            List<String> missingTopics = new ArrayList<>();
            for (InternalTopicConfig topic : topicToTaskIds.keySet()) {
                List<PartitionInfo> partitions = streamThread.restoreConsumer.partitionsFor(topic.name());
                if (partitions == null) {
                    missingTopics.add(topic.name());
                }
            }
            if (!missingTopics.isEmpty()) {
                log.warn("stream-thread [{}] Topic {} do not exists but couldn't created as the config '{}' isn't supplied",
                        streamThread.getName(), missingTopics, StreamsConfig.ZOOKEEPER_CONNECT_CONFIG);

            }
        }

        return partitionInfos;
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // This assigns tasks to consumer clients in two steps.
        // 1. using TaskAssignor to assign tasks to consumer clients.
        //    - Assign a task to a client which was running it previously.
        //      If there is no such client, assign a task to a client which has its valid local state.
        //    - A client may have more than one stream threads.
        //      The assignor tries to assign tasks to a client proportionally to the number of threads.
        //    - We try not to assign the same set of tasks to two different clients
        //    We do the assignment in one-pass. The result may not satisfy above all.
        // 2. within each client, tasks are assigned to consumer clients in round-robin manner.
        Map<UUID, Set<String>> consumersByClient = new HashMap<>();
        Map<UUID, ClientState<TaskId>> states = new HashMap<>();
        Map<UUID, HostInfo> consumerEndPointMap = new HashMap<>();
        // decode subscription info
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            Subscription subscription = entry.getValue();


            SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            if (info.userEndPoint != null) {
                final String[] hostPort = info.userEndPoint.split(":");
                consumerEndPointMap.put(info.processId, new HostInfo(hostPort[0], Integer.valueOf(hostPort[1])));
            }
            Set<String> consumers = consumersByClient.get(info.processId);
            if (consumers == null) {
                consumers = new HashSet<>();
                consumersByClient.put(info.processId, consumers);
            }
            consumers.add(consumerId);

            ClientState<TaskId> state = states.get(info.processId);
            if (state == null) {
                state = new ClientState<>();
                states.put(info.processId, state);
            }

            state.prevActiveTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.standbyTasks);
            state.capacity = state.capacity + 1d;
        }


        this.topicGroups = streamThread.builder.topicGroups();

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those internal topics.
        internalSourceTopicToTaskIds = new HashMap<>();
        Map<Integer, Set<String>> sourceTopicGroups = new HashMap<>();
        Map<Integer, Collection<InternalTopicConfig>> internalSourceTopicGroups = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            sourceTopicGroups.put(entry.getKey(), entry.getValue().sourceTopics);
            internalSourceTopicGroups.put(entry.getKey(), entry.getValue().interSourceTopics.values());
        }


        // for all internal source topics
        // set the number of partitions to the maximum of the depending sub-topologies source topics
        Map<TopicPartition, PartitionInfo> internalPartitionInfos = new HashMap<>();
        Map<String, InternalTopicConfig> allInternalTopics = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            Map<String, InternalTopicConfig> internalTopics = entry.getValue().interSourceTopics;
            allInternalTopics.putAll(internalTopics);
            for (InternalTopicConfig internalTopic : internalTopics.values()) {
                Set<TaskId> tasks = internalSourceTopicToTaskIds.get(internalTopic);

                if (tasks == null) {
                    int numPartitions = -1;
                    for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> other : topicGroups.entrySet()) {
                        Set<String> otherSinkTopics = other.getValue().sinkTopics;

                        if (otherSinkTopics.contains(internalTopic.name())) {
                            for (String topic : other.getValue().sourceTopics) {
                                Integer partitions = null;
                                // It is possible the sourceTopic is another internal topic, i.e,
                                // map().join().join(map())
                                if (allInternalTopics.containsKey(topic)) {
                                    Set<TaskId> taskIds = internalSourceTopicToTaskIds.get(allInternalTopics.get(topic));
                                    if (taskIds != null) {
                                        for (TaskId taskId : taskIds) {
                                            partitions = taskId.partition;
                                        }
                                    }
                                } else {
                                    partitions = metadata.partitionCountForTopic(topic);
                                }
                                if (partitions != null && partitions > numPartitions) {
                                    numPartitions = partitions;
                                }
                            }
                        }
                    }
                    internalSourceTopicToTaskIds.put(internalTopic, Collections.singleton(new TaskId(entry.getKey(), numPartitions)));
                    for (int partition = 0; partition < numPartitions; partition++) {
                        internalPartitionInfos.put(new TopicPartition(internalTopic.name(), partition),
                                                   new PartitionInfo(internalTopic.name(), partition, null, new Node[0], new Node[0]));
                    }
                }
            }
        }


        Collection<Set<String>> copartitionTopicGroups = streamThread.builder.copartitionGroups();
        ensureCopartitioning(copartitionTopicGroups, internalSourceTopicGroups,
                             metadata.withPartitions(internalPartitionInfos));


        internalPartitionInfos = prepareTopic(internalSourceTopicToTaskIds, false);
        internalSourceTopicToTaskIds.clear();

        metadataWithInternalTopics = metadata;
        if (internalTopicManager != null)
            metadataWithInternalTopics = metadata.withPartitions(internalPartitionInfos);

        // get the tasks as partition groups from the partition grouper
        Map<TaskId, Set<TopicPartition>> partitionsForTask = streamThread.partitionGrouper.partitionGroups(
                sourceTopicGroups, metadataWithInternalTopics);

        // add tasks to state change log topic subscribers
        stateChangelogTopicToTaskIds = new HashMap<>();
        for (TaskId task : partitionsForTask.keySet()) {
            final Map<String, InternalTopicConfig> stateChangelogTopics = topicGroups.get(task.topicGroupId).stateChangelogTopics;
            for (InternalTopicConfig topic : stateChangelogTopics.values()) {
                Set<TaskId> tasks = stateChangelogTopicToTaskIds.get(topic);
                if (tasks == null) {
                    tasks = new HashSet<>();
                    stateChangelogTopicToTaskIds.put(topic, tasks);
                }

                tasks.add(task);
            }

            final Map<String, InternalTopicConfig> interSourceTopics = topicGroups.get(task.topicGroupId).interSourceTopics;
            for (InternalTopicConfig topic : interSourceTopics.values()) {
                Set<TaskId> tasks = internalSourceTopicToTaskIds.get(topic);
                if (tasks == null) {
                    tasks = new HashSet<>();
                    internalSourceTopicToTaskIds.put(topic, tasks);
                }

                tasks.add(task);
            }
        }

        // assign tasks to clients
        states = TaskAssignor.assign(states, partitionsForTask.keySet(), numStandbyReplicas, streamThread.getName());

        final List<AssignmentSupplier> assignmentSuppliers = new ArrayList<>();

        final Map<HostInfo, Set<TopicPartition>> endPointMap = new HashMap<>();
        for (Map.Entry<UUID, Set<String>> entry : consumersByClient.entrySet()) {
            UUID processId = entry.getKey();
            Set<String> consumers = entry.getValue();
            ClientState<TaskId> state = states.get(processId);

            ArrayList<TaskId> taskIds = new ArrayList<>(state.assignedTasks.size());
            final int numActiveTasks = state.activeTasks.size();
            for (TaskId taskId : state.activeTasks) {
                taskIds.add(taskId);
            }
            for (TaskId id : state.assignedTasks) {
                if (!state.activeTasks.contains(id))
                    taskIds.add(id);
            }

            final int numConsumers = consumers.size();


            int i = 0;
            for (String consumer : consumers) {
                Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();
                ArrayList<AssignedPartition> assignedPartitions = new ArrayList<>();

                final int numTaskIds = taskIds.size();
                for (int j = i; j < numTaskIds; j += numConsumers) {
                    TaskId taskId = taskIds.get(j);
                    if (j < numActiveTasks) {
                        for (TopicPartition partition : partitionsForTask.get(taskId)) {
                            assignedPartitions.add(new AssignedPartition(taskId, partition));
                        }
                    } else {
                        Set<TopicPartition> standbyPartitions = standby.get(taskId);
                        if (standbyPartitions == null) {
                            standbyPartitions = new HashSet<>();
                            standby.put(taskId, standbyPartitions);
                        }
                        standbyPartitions.addAll(partitionsForTask.get(taskId));
                    }
                }

                Collections.sort(assignedPartitions);
                List<TaskId> active = new ArrayList<>();
                List<TopicPartition> activePartitions = new ArrayList<>();
                for (AssignedPartition partition : assignedPartitions) {
                    active.add(partition.taskId);
                    activePartitions.add(partition.partition);
                    HostInfo hostInfo = consumerEndPointMap.get(processId);
                    if (hostInfo != null) {
                        if (!endPointMap.containsKey(hostInfo)) {
                            endPointMap.put(hostInfo, new HashSet<TopicPartition>());
                        }
                        final Set<TopicPartition> topicPartitions = endPointMap.get(hostInfo);
                        topicPartitions.add(partition.partition);
                    }
                }


                assignmentSuppliers.add(new AssignmentSupplier(consumer,
                                                               active,
                                                               standby,
                                                               endPointMap,
                                                               activePartitions));

                i++;
            }
        }

        // if ZK is specified, validate the internal topics again
        prepareTopic(internalSourceTopicToTaskIds,  /* compactTopic */ true);
        // change log topics should be compacted
        prepareTopic(stateChangelogTopicToTaskIds,  /* compactTopic */ true);

        Map<String, Assignment> assignment = new HashMap<>();
        for (AssignmentSupplier assignmentSupplier : assignmentSuppliers) {
            assignment.put(assignmentSupplier.consumer, assignmentSupplier.get());
        }
        return assignment;
    }

    class AssignmentSupplier {
        private final String consumer;
        private final List<TaskId> active;
        private final Map<TaskId, Set<TopicPartition>> standby;
        private final Map<HostInfo, Set<TopicPartition>> endPointMap;
        private final List<TopicPartition> activePartitions;

        AssignmentSupplier(final String consumer,
                           final List<TaskId> active,
                           final Map<TaskId, Set<TopicPartition>> standby,
                           final Map<HostInfo, Set<TopicPartition>> endPointMap,
                           final List<TopicPartition> activePartitions) {
            this.consumer = consumer;
            this.active = active;
            this.standby = standby;
            this.endPointMap = endPointMap;
            this.activePartitions = activePartitions;
        }

        Assignment get() {
            return new Assignment(activePartitions, new AssignmentInfo(active,
                                                                       standby,
                                                                       endPointMap).encode());
        }
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(Assignment assignment) {
        List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());

        Collections.sort(partitions, PARTITION_COMPARATOR);

        AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
        this.standbyTasks = info.standbyTasks;

        Map<TopicPartition, Set<TaskId>> partitionToTaskIds = new HashMap<>();
        Iterator<TaskId> iter = info.activeTasks.iterator();
        for (TopicPartition partition : partitions) {
            Set<TaskId> taskIds = partitionToTaskIds.get(partition);
            if (taskIds == null) {
                taskIds = new HashSet<>();
                partitionToTaskIds.put(partition, taskIds);
            }

            if (iter.hasNext()) {
                taskIds.add(iter.next());
            } else {
                TaskAssignmentException ex = new TaskAssignmentException(
                        String.format("stream-thread [%s] failed to find a task id for the partition=%s" +
                        ", partitions=%d, assignmentInfo=%s", streamThread.getName(), partition.toString(), partitions.size(), info.toString())
                );
                log.error(ex.getMessage(), ex);
                throw ex;
            }
        }
        this.partitionToTaskIds = partitionToTaskIds;
        this.partitionsByHostState = info.partitionsByHostState;
        // only need to build when not coordinator
        if (metadataWithInternalTopics == null) {
            final Collection<Set<TopicPartition>> values = partitionsByHostState.values();
            final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
            for (Set<TopicPartition> value : values) {
                for (TopicPartition topicPartition : value) {
                    topicToPartitionInfo.put(topicPartition, new PartitionInfo(topicPartition.topic(),
                                                                               topicPartition.partition(),
                                                                               null,
                                                                               new Node[0],
                                                                               new Node[0]));
                }
            }
            metadataWithInternalTopics = Cluster.empty().withPartitions(topicToPartitionInfo);
        }
    }

    public Map<HostInfo, Set<TopicPartition>> getPartitionsByHostState() {
        if (partitionsByHostState == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(partitionsByHostState);
    }

    public Cluster clusterMetadata() {
        if (metadataWithInternalTopics == null) {
            return Cluster.empty();
        }
        return metadataWithInternalTopics;
    }

    private void ensureCopartitioning(Collection<Set<String>> copartitionGroups, Map<Integer, Collection<InternalTopicConfig>> internalTopicGroups, Cluster metadata) {
        Map<String, InternalTopicConfig> internalTopics = new HashMap<>();
        for (Collection<InternalTopicConfig> topics : internalTopicGroups.values()) {
            for (InternalTopicConfig topic : topics) {
                internalTopics.put(topic.name(), topic);
            }
        }

        for (Set<String> copartitionGroup : copartitionGroups) {
            ensureCopartitioning(copartitionGroup, internalTopics, metadata);
        }
    }

    private void ensureCopartitioning(Set<String> copartitionGroup, Map<String, InternalTopicConfig> internalTopics, Cluster metadata) {
        int numPartitions = -1;

        for (String topic : copartitionGroup) {
            if (!internalTopics.containsKey(topic)) {
                List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

                if (infos == null)
                    throw new TopologyBuilderException(String.format("stream-thread [%s] External source topic not found: %s", streamThread.getName(), topic));

                if (numPartitions == -1) {
                    numPartitions = infos.size();
                } else if (numPartitions != infos.size()) {
                    String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                    Arrays.sort(topics);
                    throw new TopologyBuilderException(String.format("stream-thread [%s] Topics not copartitioned: [%s]", streamThread.getName(), Utils.mkString(Arrays.asList(topics), ",")));
                }
            }
        }

        if (numPartitions == -1) {
            for (InternalTopicConfig topic : internalTopics.values()) {
                if (copartitionGroup.contains(topic.name())) {
                    Integer partitions = metadata.partitionCountForTopic(topic.name());
                    if (partitions != null && partitions > numPartitions) {
                        numPartitions = partitions;
                    }
                }
            }
        }
        // enforce co-partitioning restrictions to internal topics reusing internalSourceTopicToTaskIds
        for (InternalTopicConfig topic : internalTopics.values()) {
            if (copartitionGroup.contains(topic.name())) {
                internalSourceTopicToTaskIds
                    .put(topic, Collections.singleton(new TaskId(-1, numPartitions)));
            }
        }
    }

    /* For Test Only */
    public Set<TaskId> tasksForState(String stateName) {
        final String changeLogName = ProcessorStateManager.storeChangelogTopic(streamThread.applicationId, stateName);
        for (InternalTopicConfig internalTopicConfig : stateChangelogTopicToTaskIds.keySet()) {
            if (internalTopicConfig.name().equals(changeLogName)) {
                return stateChangelogTopicToTaskIds.get(internalTopicConfig);
            }
        }
        return Collections.emptySet();
    }

    public Set<TaskId> tasksForPartition(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }

    public void setInternalTopicManager(InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     */
    public static  class SubscriptionUpdates {

        private final Set<String> updatedTopicSubscriptions = new HashSet<>();


        private  void updateTopics(Collection<String> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<String> getUpdates() {
            return Collections.unmodifiableSet(new HashSet<>(updatedTopicSubscriptions));
        }

        public boolean hasUpdates() {
            return !updatedTopicSubscriptions.isEmpty();
        }

    }

}
