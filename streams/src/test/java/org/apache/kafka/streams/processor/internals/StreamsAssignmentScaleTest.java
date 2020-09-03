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

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.createMockAdminClientForAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getInfo;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class StreamsAssignmentScaleTest {
    final static long MAX_ASSIGNMENT_DURATION = 60 * 1000L; //each individual assignment should complete within 20s
    final static String APPLICATION_ID = "streams-assignment-scale-test";

    private final Logger log = LoggerFactory.getLogger(StreamsAssignmentScaleTest.class);

    private final Class<? extends TaskAssignor> taskAssignor;

    @Parameterized.Parameters(name = "task assignor = {0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[]{StickyTaskAssignor.class},
            new Object[]{HighAvailabilityTaskAssignor.class},
            new Object[]{FallbackPriorTaskAssignor.class}
        );
    }

    public StreamsAssignmentScaleTest(final Class<? extends TaskAssignor> taskAssignor) {
        this.taskAssignor = taskAssignor;
    }

    @Test(timeout = 120 * 1000)
    public void testLargePartitionCount() {
        shouldCompleteLargeAssignmentInReasonableTime(3_000, 1, 1, 1);
    }

    @Test(timeout = 120 * 1000)
    public void testLargeNumConsumers() {
        shouldCompleteLargeAssignmentInReasonableTime(1_000, 1_000, 1, 1);
    }

    @Test(timeout = 120 * 1000)
    public void testManyStandbys() {
        shouldCompleteLargeAssignmentInReasonableTime(1_000, 100, 1, 50);
    }

    @Test(timeout = 120 * 1000)
    public void testManyThreadsPerClient() {
        shouldCompleteLargeAssignmentInReasonableTime(1_000, 10, 1000, 1);
    }

    private void shouldCompleteLargeAssignmentInReasonableTime(final int numPartitions,
                                                               final int numClients,
                                                               final int numThreadsPerClient,
                                                               final int numStandbys) {
        final List<String> topic = singletonList("topic");
        final Map<TopicPartition, Long> changelogEndOffsets = new HashMap<>();
        for (int p = 0; p < numPartitions; ++p) {
            changelogEndOffsets.put(new TopicPartition(APPLICATION_ID + "-store-changelog", p), 100_000L);
        }
        final List<PartitionInfo> partitionInfos = getPartitionInfos(numPartitions);
        final Cluster clusterMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            partitionInfos,
            emptySet(),
            emptySet()
        );

        final InternalTopologyBuilder builder = new InternalTopologyBuilder();
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor");
        builder.setApplicationId(APPLICATION_ID);
        builder.buildTopology();

        final Consumer<byte[], byte[]> mainConsumer = EasyMock.createNiceMock(Consumer.class);
        final TaskManager taskManager = EasyMock.createNiceMock(TaskManager.class);
        expect(taskManager.builder()).andReturn(builder).anyTimes();
        expect(taskManager.mainConsumer()).andStubReturn(mainConsumer);
        expect(mainConsumer.committed(new HashSet<>())).andStubReturn(Collections.emptyMap());
        final AdminClient adminClient = createMockAdminClientForAssignor(changelogEndOffsets);

        final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8080");
        configMap.put(InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        configMap.put(InternalConfig.STREAMS_METADATA_STATE_FOR_PARTITION_ASSIGNOR, EasyMock.createNiceMock(StreamsMetadataState.class));
        configMap.put(InternalConfig.STREAMS_ADMIN_CLIENT, adminClient);
        configMap.put(InternalConfig.ASSIGNMENT_ERROR_CODE, new AtomicInteger());
        configMap.put(InternalConfig.NEXT_SCHEDULED_REBALANCE_MS, new AtomicLong(Long.MAX_VALUE));
        configMap.put(InternalConfig.TIME, new MockTime());
        configMap.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, taskAssignor.getName());
        configMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbys);

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            new MockTime(),
            new StreamsConfig(configMap),
            new MockClientSupplier().restoreConsumer,
            false
        );
        partitionAssignor.configure(configMap);
        EasyMock.replay(taskManager, adminClient, mainConsumer);

        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        final Map<String, Subscription> subscriptions = new HashMap<>();
        for (int client = 0; client < numClients; ++client) {
            for (int i = 0; i < numThreadsPerClient; ++i) {
                subscriptions.put("consumer-" + client + "-" + i,
                                  new Subscription(
                                      topic,
                                      getInfo(uuidForInt(client), EMPTY_TASKS, EMPTY_TASKS).encode())
                );
            }
        }

        final long firstAssignmentStartMs = System.currentTimeMillis();
        final Map<String, Assignment> firstAssignments = partitionAssignor.assign(clusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        final long firstAssignmentEndMs = System.currentTimeMillis();

        final long firstAssignmentDuration = firstAssignmentEndMs - firstAssignmentStartMs;
        if (firstAssignmentDuration > MAX_ASSIGNMENT_DURATION) {
            throw new AssertionError("The first assignment took took too long to complete at " + firstAssignmentDuration + "ms.");
        } else {
            log.info("First assignment took {}ms.", firstAssignmentDuration);
        }

        // Use the assignment to generate the subscriptions' prev task data for the next rebalance
        for (int client = 0; client < numClients; ++client) {
            for (int i = 0; i < numThreadsPerClient; ++i) {
                final String consumer = "consumer-" + client + "-" + i;
                final Assignment assignment = firstAssignments.get(consumer);
                final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

                subscriptions.put(consumer,
                                  new Subscription(
                                      topic,
                                      getInfo(uuidForInt(i), new HashSet<>(info.activeTasks()), info.standbyTasks().keySet()).encode(),
                                      assignment.partitions())
                );
            }
        }

        final long secondAssignmentStartMs = System.currentTimeMillis();
        final Map<String, Assignment> secondAssignments = partitionAssignor.assign(clusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        final long secondAssignmentEndMs = System.currentTimeMillis();
        final long secondAssignmentDuration = secondAssignmentEndMs - secondAssignmentStartMs;
        if (secondAssignmentDuration > MAX_ASSIGNMENT_DURATION) {
            throw new AssertionError("The second assignment took took too long to complete at " + secondAssignmentDuration + "ms.");
        } else {
            log.info("Second assignment took {}ms.", secondAssignmentDuration);
        }

        assertThat(secondAssignments.size(), is(numClients * numThreadsPerClient));
    }

    private static List<PartitionInfo> getPartitionInfos(final int numPartitionsPerTopic) {
        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        for (int p = 0; p < numPartitionsPerTopic; ++p) {
            partitionInfos.add(new PartitionInfo("topic", p, Node.noNode(), new Node[0], new Node[0]));
        }
        return  partitionInfos;
    }
}
