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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CHANGELOG_END_OFFSETS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_6;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_7;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_8;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_9;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RackAwarenessStreamsPartitionAssignorTest {

    private final List<PartitionInfo> infos = asList(
        new PartitionInfo("topic0", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic0", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic0", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic1", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic2", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic3", 2, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic4", 0, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic4", 1, Node.noNode(), new Node[0], new Node[0]),
        new PartitionInfo("topic4", 2, Node.noNode(), new Node[0], new Node[0])
    );

    final String consumer1 = "consumer1";
    final String consumer2 = "consumer2";
    final String consumer3 = "consumer3";
    final String consumer4 = "consumer4";
    final String consumer5 = "consumer5";
    final String consumer6 = "consumer6";
    final String consumer7 = "consumer7";
    final String consumer8 = "consumer8";
    final String consumer9 = "consumer9";


    private final Cluster metadata = new Cluster(
            "cluster",
            singletonList(Node.noNode()),
            infos,
            emptySet(),
            emptySet());

    private final static List<String> ALL_TAG_KEYS = new ArrayList<>();
    static {
        for (int i = 0; i < StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE; i++) {
            ALL_TAG_KEYS.add("key-" + i);
        }
    }

    private final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private TaskManager taskManager;
    private Admin adminClient;
    private StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private TopologyMetadata topologyMetadata = new TopologyMetadata(builder, streamsConfig);
    private final StreamsMetadataState streamsMetadataState = mock(StreamsMetadataState.class);
    private final Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new HashMap<>();
    private final MockTime time = new MockTime();

    @SuppressWarnings("unchecked")
    private Map<String, Object> configProps() {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);
        final ReferenceContainer referenceContainer = new ReferenceContainer();
        referenceContainer.mainConsumer = (Consumer<byte[], byte[]>) mock(Consumer.class);
        referenceContainer.adminClient = adminClient;
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        configurationMap.put(StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        configurationMap.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", ALL_TAG_KEYS));
        ALL_TAG_KEYS.forEach(key -> configurationMap.put(StreamsConfig.clientTagPrefix(key), "dummy"));
        return configurationMap;
    }

    // Make sure to complete setting up any mocks (such as TaskManager or AdminClient) before configuring the assignor
    private void configurePartitionAssignorWith(final Map<String, Object> props) {
        final Map<String, Object> configMap = configProps();
        configMap.putAll(props);

        streamsConfig = new StreamsConfig(configMap);
        topologyMetadata = new TopologyMetadata(builder, streamsConfig);
        partitionAssignor.configure(configMap);

        overwriteInternalTopicManagerWithMock();
    }

    // Useful for tests that don't care about the task offset sums
    private void createMockTaskManager() {
        taskManager = mock(TaskManager.class);
        when(taskManager.topologyMetadata()).thenReturn(topologyMetadata);
        when(taskManager.processId()).thenReturn(UUID_1);
        topologyMetadata.buildAndRewriteTopology();
    }

    // If you don't care about setting the end offsets for each specific topic partition, the helper method
    // getTopicPartitionOffsetMap is useful for building this input map for all partitions
    private void createMockAdminClient(final Map<TopicPartition, Long> changelogEndOffsets) {
        adminClient = mock(AdminClient.class);

        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(changelogEndOffsets.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                t -> {
                    final ListOffsetsResult.ListOffsetsResultInfo info = mock(ListOffsetsResult.ListOffsetsResultInfo.class);
                    when(info.offset()).thenReturn(t.getValue());
                    return info;
                }))
        );

        when(adminClient.listOffsets(any())).thenReturn(result);
        when(result.all()).thenReturn(allFuture);
    }

    private void overwriteInternalTopicManagerWithMock() {
        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
                time,
                streamsConfig,
                mockClientSupplier.restoreConsumer,
                false
        );
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);
    }

    @Before
    public void setUp() {
        createMockAdminClient(EMPTY_CHANGELOG_END_OFFSETS);
    }

    @Test
    public void shouldDistributeWithMaximumNumberOfClientTags() {
        setupTopology(3, 2);

        createMockTaskManager();
        createMockAdminClient(getTopicPartitionOffsetsMap(
            Arrays.asList(APPLICATION_ID + "-store2-changelog", APPLICATION_ID + "-store3-changelog", APPLICATION_ID + "-store4-changelog"),
            Arrays.asList(3, 3, 3)));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        final Map<String, String> clientTags1 = new HashMap<>();
        final Map<String, String> clientTags2 = new HashMap<>();

        for (int i = 0; i < ALL_TAG_KEYS.size(); i++) {
            final String key = ALL_TAG_KEYS.get(i);
            clientTags1.put(key, "value-1-" + i);
            clientTags2.put(key, "value-2-" + i);
        }

        final Map<String, Map<String, String>> hostTags = new HashMap<>();
        subscriptions.put(consumer1, getSubscription(UUID_1, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer1, clientTags1);
        subscriptions.put(consumer2, getSubscription(UUID_2, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer2, clientTags1);
        subscriptions.put(consumer3, getSubscription(UUID_3, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer3, clientTags2);

        Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor
            .assign(metadata, new ConsumerPartitionAssignor.GroupSubscription(subscriptions))
            .groupAssignment();

        verifyIdealTaskDistributionReached(getClientTagDistributions(assignments, hostTags), ALL_TAG_KEYS);

        // kill the first consumer and rebalance, should still achieve ideal distribution
        subscriptions.clear();
        subscriptions.put(consumer2, getSubscription(UUID_2, AssignmentInfo.decode(assignments.get(consumer2).userData()).activeTasks(), clientTags1));
        subscriptions.put(consumer3, getSubscription(UUID_3, AssignmentInfo.decode(assignments.get(consumer3).userData()).activeTasks(), clientTags2));

        assignments = partitionAssignor.assign(metadata, new ConsumerPartitionAssignor.GroupSubscription(subscriptions))
            .groupAssignment();

        verifyIdealTaskDistributionReached(getClientTagDistributions(assignments, hostTags), ALL_TAG_KEYS);
    }

    @Test
    public void shouldDistributeOnDistinguishingTagSubset() {
        setupTopology(3, 0);

        createMockTaskManager();
        createMockAdminClient(getTopicPartitionOffsetsMap(
            Arrays.asList(APPLICATION_ID + "-store0-changelog", APPLICATION_ID + "-store1-changelog", APPLICATION_ID + "-store2-changelog"),
            Arrays.asList(3, 3, 3)));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1));

        // use the same tag value for key1, and different value for key2
        // then we verify that for key2 we still achieve ideal distribution
        final Map<String, String> clientTags1 = new HashMap<>();
        final Map<String, String> clientTags2 = new HashMap<>();
        clientTags1.put(ALL_TAG_KEYS.get(0), "value-1-all");
        clientTags2.put(ALL_TAG_KEYS.get(0), "value-2-all");
        clientTags1.put(ALL_TAG_KEYS.get(1), "value-1-1");
        clientTags2.put(ALL_TAG_KEYS.get(1), "value-2-2");

        final String consumer1 = "consumer1";
        final String consumer2 = "consumer2";
        final String consumer3 = "consumer3";
        final String consumer4 = "consumer4";
        final String consumer5 = "consumer5";
        final String consumer6 = "consumer6";

        final Map<String, Map<String, String>> hostTags = new HashMap<>();
        subscriptions.put(consumer1, getSubscription(UUID_1, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer1, clientTags1);
        subscriptions.put(consumer2, getSubscription(UUID_2, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer2, clientTags1);
        subscriptions.put(consumer3, getSubscription(UUID_3, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer3, clientTags1);
        subscriptions.put(consumer4, getSubscription(UUID_4, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer4, clientTags2);
        subscriptions.put(consumer5, getSubscription(UUID_5, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer5, clientTags2);
        subscriptions.put(consumer6, getSubscription(UUID_6, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer6, clientTags2);

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor
            .assign(metadata, new ConsumerPartitionAssignor.GroupSubscription(subscriptions))
            .groupAssignment();

        verifyIdealTaskDistributionReached(getClientTagDistributions(assignments, hostTags), Collections.singletonList(ALL_TAG_KEYS.get(1)));
    }

    @Test
    public void shouldDistributeWithMultipleStandbys() {
        setupTopology(3, 0);

        createMockTaskManager();
        createMockAdminClient(getTopicPartitionOffsetsMap(
            Arrays.asList(APPLICATION_ID + "-store0-changelog", APPLICATION_ID + "-store1-changelog", APPLICATION_ID + "-store2-changelog"),
            Arrays.asList(3, 3, 3)));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2));

        final Map<String, String> clientTags1 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-1"));
        final Map<String, String> clientTags2 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-2"));
        final Map<String, String> clientTags3 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-3"));
        final Map<String, String> clientTags4 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-1"));
        final Map<String, String> clientTags5 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-2"));
        final Map<String, String> clientTags6 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-3"));
        final Map<String, String> clientTags7 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-3"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-1"));
        final Map<String, String> clientTags8 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-3"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-2"));
        final Map<String, String> clientTags9 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-3"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-3"));

        final Map<String, Map<String, String>> hostTags = new HashMap<>();
        subscriptions.put(consumer1, getSubscription(UUID_1, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer1, clientTags1);
        subscriptions.put(consumer2, getSubscription(UUID_2, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer2, clientTags2);
        subscriptions.put(consumer3, getSubscription(UUID_3, EMPTY_TASKS, clientTags3));
        hostTags.put(consumer3, clientTags3);
        subscriptions.put(consumer4, getSubscription(UUID_4, EMPTY_TASKS, clientTags4));
        hostTags.put(consumer4, clientTags4);
        subscriptions.put(consumer5, getSubscription(UUID_5, EMPTY_TASKS, clientTags5));
        hostTags.put(consumer5, clientTags5);
        subscriptions.put(consumer6, getSubscription(UUID_6, EMPTY_TASKS, clientTags6));
        hostTags.put(consumer6, clientTags6);
        subscriptions.put(consumer7, getSubscription(UUID_7, EMPTY_TASKS, clientTags7));
        hostTags.put(consumer7, clientTags7);
        subscriptions.put(consumer8, getSubscription(UUID_8, EMPTY_TASKS, clientTags8));
        hostTags.put(consumer8, clientTags8);
        subscriptions.put(consumer9, getSubscription(UUID_9, EMPTY_TASKS, clientTags9));
        hostTags.put(consumer9, clientTags9);

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor
            .assign(metadata, new ConsumerPartitionAssignor.GroupSubscription(subscriptions))
            .groupAssignment();

        verifyIdealTaskDistributionReached(getClientTagDistributions(assignments, hostTags), Arrays.asList(ALL_TAG_KEYS.get(0), ALL_TAG_KEYS.get(1)));
    }

    @Test
    public void shouldDistributePartiallyWhenDoNotHaveEnoughClients() {
        setupTopology(3, 0);

        createMockTaskManager();
        createMockAdminClient(getTopicPartitionOffsetsMap(
            Arrays.asList(APPLICATION_ID + "-store0-changelog", APPLICATION_ID + "-store1-changelog", APPLICATION_ID + "-store2-changelog"),
            Arrays.asList(3, 3, 3)));
        configurePartitionAssignorWith(Collections.singletonMap(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2));

        final Map<String, String> clientTags1 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-1"));
        final Map<String, String> clientTags2 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-2"));
        final Map<String, String> clientTags3 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-1"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-3"));
        final Map<String, String> clientTags4 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-1"));
        final Map<String, String> clientTags5 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-2"));
        final Map<String, String> clientTags6 = mkMap(
            mkEntry(ALL_TAG_KEYS.get(0), "value-0-2"),
            mkEntry(ALL_TAG_KEYS.get(1), "value-1-3"));

        final Map<String, Map<String, String>> hostTags = new HashMap<>();
        subscriptions.put(consumer1, getSubscription(UUID_1, EMPTY_TASKS, clientTags1));
        hostTags.put(consumer1, clientTags1);
        subscriptions.put(consumer2, getSubscription(UUID_2, EMPTY_TASKS, clientTags2));
        hostTags.put(consumer2, clientTags2);
        subscriptions.put(consumer3, getSubscription(UUID_3, EMPTY_TASKS, clientTags3));
        hostTags.put(consumer3, clientTags3);
        subscriptions.put(consumer4, getSubscription(UUID_4, EMPTY_TASKS, clientTags4));
        hostTags.put(consumer4, clientTags4);
        subscriptions.put(consumer5, getSubscription(UUID_5, EMPTY_TASKS, clientTags5));
        hostTags.put(consumer5, clientTags5);
        subscriptions.put(consumer6, getSubscription(UUID_6, EMPTY_TASKS, clientTags6));
        hostTags.put(consumer6, clientTags6);

        final Map<String, ConsumerPartitionAssignor.Assignment> assignments = partitionAssignor
            .assign(metadata, new ConsumerPartitionAssignor.GroupSubscription(subscriptions))
            .groupAssignment();

        verifyIdealTaskDistributionReached(getClientTagDistributions(assignments, hostTags), Collections.singletonList(ALL_TAG_KEYS.get(1)));
        verifyPartialTaskDistributionReached(getClientTagDistributions(assignments, hostTags), Collections.singletonList(ALL_TAG_KEYS.get(0)));
    }

    private Map<TaskId, ClientTagDistribution> getClientTagDistributions(final Map<String, ConsumerPartitionAssignor.Assignment> assignments,
                                                                         final Map<String, Map<String, String>> hostTags) {
        final Map<TaskId, ClientTagDistribution> taskClientTags = new HashMap<>();

        for (final Map.Entry<String, ConsumerPartitionAssignor.Assignment> entry : assignments.entrySet()) {
            final AssignmentInfo info = AssignmentInfo.decode(entry.getValue().userData());

            for (final TaskId activeTaskId : info.activeTasks()) {
                taskClientTags.putIfAbsent(activeTaskId, new ClientTagDistribution(activeTaskId));
                final ClientTagDistribution tagDistribution = taskClientTags.get(activeTaskId);
                tagDistribution.addActiveTags(hostTags.get(entry.getKey()));
            }

            for (final TaskId standbyTaskId : info.standbyTasks().keySet()) {
                taskClientTags.putIfAbsent(standbyTaskId, new ClientTagDistribution(standbyTaskId));
                final ClientTagDistribution tagDistribution = taskClientTags.get(standbyTaskId);
                tagDistribution.addStandbyTags(hostTags.get(entry.getKey()));
            }
        }

        return taskClientTags;
    }

    private void verifyIdealTaskDistributionReached(final Map<TaskId, ClientTagDistribution> taskClientTags,
                                                    final List<String> tagsToCheck) {
        for (final Map.Entry<TaskId, ClientTagDistribution> entry: taskClientTags.entrySet()) {
            if (!tagsAmongStandbysAreDifferent(entry.getValue(), tagsToCheck))
                throw new AssertionError("task " + entry.getKey() + "'s tag-distribution for " + tagsToCheck +
                    " among standbys is not ideal: " + entry.getValue());

            if (!tagsAmongActiveAndAllStandbysAreDifferent(entry.getValue(), tagsToCheck))
                throw new AssertionError("task " + entry.getKey() + "'s tag-distribution for " + tagsToCheck +
                    " between active and standbys is not ideal: " + entry.getValue());
        }
    }

    private void verifyPartialTaskDistributionReached(final Map<TaskId, ClientTagDistribution> taskClientTags,
                                                      final List<String> tagsToCheck) {
        for (final Map.Entry<TaskId, ClientTagDistribution> entry: taskClientTags.entrySet()) {
            if (!tagsAmongActiveAndAtLeastOneStandbyIsDifferent(entry.getValue(), tagsToCheck))
                throw new AssertionError("task " + entry.getKey() + "'s tag-distribution for " + tagsToCheck +
                    "between active and standbys is not partially ideal: " + entry.getValue());
        }
    }

    private static boolean tagsAmongActiveAndAllStandbysAreDifferent(final ClientTagDistribution tagDistribution,
                                                                     final List<String> tagsToCheck) {
        return tagDistribution.standbysClientTags.stream().allMatch(standbyTags ->
            tagsToCheck.stream().noneMatch(tag -> tagDistribution.activeClientTags.get(tag).equals(standbyTags.get(tag))));
    }

    private static boolean tagsAmongActiveAndAtLeastOneStandbyIsDifferent(final ClientTagDistribution tagDistribution,
                                                                          final List<String> tagsToCheck) {
        return tagDistribution.standbysClientTags.stream().anyMatch(standbyTags ->
            tagsToCheck.stream().noneMatch(tag -> tagDistribution.activeClientTags.get(tag).equals(standbyTags.get(tag))));
    }

    private static boolean tagsAmongStandbysAreDifferent(final ClientTagDistribution tagDistribution,
                                                         final List<String> tagsToCheck) {
        final Map<String, Integer> statistics = new HashMap<>();

        for (final Map<String, String> tags : tagDistribution.standbysClientTags) {
            for (final Map.Entry<String, String> tag : tags.entrySet()) {
                if (tagsToCheck.contains(tag.getKey())) {
                    final String tagValue = tag.getValue();
                    final Integer tagValueOccurrence = statistics.getOrDefault(tagValue, 0);
                    statistics.put(tagValue, tagValueOccurrence + 1);
                }
            }
        }

        return statistics.values().stream().noneMatch(occurrence -> occurrence > 1);
    }

    private void setupTopology(final int numOfStatefulTopologies, final int numOfStatelessTopologies) {
        if (numOfStatefulTopologies + numOfStatelessTopologies > 5) {
            throw new IllegalArgumentException("Should not have more than 5 topologies, but have " + numOfStatefulTopologies);
        }

        for (int i = 0; i < numOfStatelessTopologies; i++) {
            builder.addSource(null, "source" + i, null, null, null, "topic" + i);
            builder.addProcessor("processor" + i, new MockApiProcessorSupplier<>(), "source" + i);
        }

        for (int i = numOfStatelessTopologies; i < numOfStatelessTopologies + numOfStatefulTopologies; i++) {
            builder.addSource(null, "source" + i, null, null, null, "topic" + i);
            builder.addProcessor("processor" + i, new MockApiProcessorSupplier<>(), "source" + i);
            builder.addStateStore(new MockKeyValueStoreBuilder("store" + i, false), "processor" + i);
        }
    }

    private static final class ClientTagDistribution {
        private final TaskId taskId;
        private final Map<String, String> activeClientTags;
        private final List<Map<String, String>> standbysClientTags;

        ClientTagDistribution(final TaskId taskId) {
            this.taskId = taskId;
            this.activeClientTags = new HashMap<>();
            this.standbysClientTags = new ArrayList<>();
        }

        void addActiveTags(final Map<String, String> activeClientTags) {
            if (!this.activeClientTags.isEmpty()) {
                throw new IllegalStateException("Found multiple active tasks for " + taskId + ", this should not happen");
            }
            this.activeClientTags.putAll(activeClientTags);
        }

        void addStandbyTags(final Map<String, String> standbyClientTags) {
            this.standbysClientTags.add(standbyClientTags);
        }

        @Override
        public String toString() {
            return "ClientTagDistribution{" +
                "taskId=" + taskId +
                ", activeClientTags=" + activeClientTags +
                ", standbysClientTags=" + standbysClientTags +
                '}';
        }
    }

    /**
     * Helper for building the input to createMockAdminClient in cases where we don't care about the actual offsets
     * @param changelogTopics The names of all changelog topics in the topology
     * @param topicsNumPartitions The number of partitions for the corresponding changelog topic, such that the number
     *            of partitions of the ith topic in changelogTopics is given by the ith element of topicsNumPartitions
     */
    private static Map<TopicPartition, Long> getTopicPartitionOffsetsMap(final List<String> changelogTopics,
                                                                         final List<Integer> topicsNumPartitions) {
        if (changelogTopics.size() != topicsNumPartitions.size()) {
            throw new IllegalStateException("Passed in " + changelogTopics.size() + " changelog topic names, but " +
                    topicsNumPartitions.size() + " different numPartitions for the topics");
        }
        final Map<TopicPartition, Long> changelogEndOffsets = new HashMap<>();
        for (int i = 0; i < changelogTopics.size(); ++i) {
            final String topic = changelogTopics.get(i);
            final int numPartitions = topicsNumPartitions.get(i);
            for (int partition = 0; partition < numPartitions; ++partition) {
                changelogEndOffsets.put(new TopicPartition(topic, partition), Long.MAX_VALUE);
            }
        }
        return changelogEndOffsets;
    }

    private static ConsumerPartitionAssignor.Subscription getSubscription(final UUID processId,
                                                                          final Collection<TaskId> prevActiveTasks,
                                                                          final Map<String, String> clientTags) {
        return new ConsumerPartitionAssignor.Subscription(
            singletonList("source1"),
            new SubscriptionInfo(LATEST_SUPPORTED_VERSION, LATEST_SUPPORTED_VERSION, processId, null,
                getTaskOffsetSums(prevActiveTasks), (byte) 0, 0, clientTags).encode()
        );
    }

    // Stub offset sums for when we only care about the prev/standby task sets, not the actual offsets
    private static Map<TaskId, Long> getTaskOffsetSums(final Collection<TaskId> activeTasks) {
        final Map<TaskId, Long> taskOffsetSums = activeTasks.stream().collect(Collectors.toMap(t -> t, t -> Task.LATEST_OFFSET));
        taskOffsetSums.putAll(EMPTY_TASKS.stream().collect(Collectors.toMap(t -> t, t -> 0L)));
        return taskOffsetSums;
    }
}
