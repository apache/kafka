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
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.createMockAdminClientForAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getInfo;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class StreamsAssignmentScaleTest {
    final static long MAX_ASSIGNMENT_DURATION = 60 * 1000L; //each individual assignment should complete within 20s
    final static String APPLICATION_ID = "streams-assignment-scale-test";

    private final Logger log = LoggerFactory.getLogger(StreamsAssignmentScaleTest.class);

    /* HighAvailabilityTaskAssignor tests */

    @Test(timeout = 120 * 1000)
    public void testHighAvailabilityTaskAssignorLargePartitionCount() {
        completeLargeAssignment(6_000, 2, 1, 1, HighAvailabilityTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testHighAvailabilityTaskAssignorLargeNumConsumers() {
        completeLargeAssignment(1_000, 1_000, 1, 1, HighAvailabilityTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testHighAvailabilityTaskAssignorManyStandbys() {
        completeLargeAssignment(1_000, 100, 1, 50, HighAvailabilityTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testHighAvailabilityTaskAssignorManyThreadsPerClient() {
        completeLargeAssignment(1_000, 10, 1000, 1, HighAvailabilityTaskAssignor.class);
    }

    /* StickyTaskAssignor tests */

    @Test(timeout = 120 * 1000)
    public void testStickyTaskAssignorLargePartitionCount() {
        completeLargeAssignment(2_000, 2, 1, 1, StickyTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testStickyTaskAssignorLargeNumConsumers() {
        completeLargeAssignment(1_000, 1_000, 1, 1, StickyTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testStickyTaskAssignorManyStandbys() {
        completeLargeAssignment(1_000, 100, 1, 20, StickyTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testStickyTaskAssignorManyThreadsPerClient() {
        completeLargeAssignment(1_000, 10, 1000, 1, StickyTaskAssignor.class);
    }

    /* FallbackPriorTaskAssignor tests */

    @Test(timeout = 120 * 1000)
    public void testFallbackPriorTaskAssignorLargePartitionCount() {
        completeLargeAssignment(2_000, 2, 1, 1, FallbackPriorTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testFallbackPriorTaskAssignorLargeNumConsumers() {
        completeLargeAssignment(1_000, 1_000, 1, 1, FallbackPriorTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testFallbackPriorTaskAssignorManyStandbys() {
        completeLargeAssignment(1_000, 100, 1, 20, FallbackPriorTaskAssignor.class);
    }

    @Test(timeout = 120 * 1000)
    public void testFallbackPriorTaskAssignorManyThreadsPerClient() {
        completeLargeAssignment(1_000, 10, 1000, 1, FallbackPriorTaskAssignor.class);
    }

    private void completeLargeAssignment(final int numPartitions,
                                         final int numClients,
                                         final int numThreadsPerClient,
                                         final int numStandbys,
                                         final Class<? extends TaskAssignor> taskAssignor) {
        final List<String> topic = singletonList("topic");

        final Map<TopicPartition, Long> changelogEndOffsets = new HashMap<>();
        for (int p = 0; p < numPartitions; ++p) {
            changelogEndOffsets.put(new TopicPartition(APPLICATION_ID + "-store-changelog", p), 100_000L);
        }

        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        for (int p = 0; p < numPartitions; ++p) {
            partitionInfos.add(new PartitionInfo("topic", p, Node.noNode(), new Node[0], new Node[0]));
        }

        final Cluster clusterMetadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            partitionInfos,
            emptySet(),
            emptySet()
        );
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8080");
        final InternalTopologyBuilder builder = new InternalTopologyBuilder();
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockApiProcessorSupplier<>(), "source");
        builder.addStateStore(new MockKeyValueStoreBuilder("store", false), "processor");
        final TopologyMetadata topologyMetadata = new TopologyMetadata(builder, new StreamsConfig(configMap));
        topologyMetadata.buildAndRewriteTopology();

        @SuppressWarnings("unchecked")
        final Consumer<byte[], byte[]> mainConsumer = mock(Consumer.class);
        final TaskManager taskManager = mock(TaskManager.class);
        when(taskManager.topologyMetadata()).thenReturn(topologyMetadata);
        when(mainConsumer.committed(anySet())).thenReturn(Collections.emptyMap());
        final AdminClient adminClient = createMockAdminClientForAssignor(changelogEndOffsets);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        referenceContainer.mainConsumer = mainConsumer;
        referenceContainer.adminClient = adminClient;
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = mock(StreamsMetadataState.class);
        referenceContainer.time = new MockTime();
        configMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        configMap.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, taskAssignor.getName());
        configMap.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbys);

        final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            new MockTime(),
            new StreamsConfig(configMap),
            new MockClientSupplier().restoreConsumer,
            false
        );

        final StreamsPartitionAssignor partitionAssignor = new StreamsPartitionAssignor();
        partitionAssignor.configure(configMap);
        partitionAssignor.setInternalTopicManager(mockInternalTopicManager);

        final Map<String, Subscription> subscriptions = new HashMap<>();
        for (int client = 0; client < numClients; ++client) {
            for (int i = 0; i < numThreadsPerClient; ++i) {
                subscriptions.put(
                    getConsumerName(i, client),
                    new Subscription(topic, getInfo(uuidForInt(client), EMPTY_TASKS, EMPTY_TASKS).encode())
                );
            }
        }

        final long firstAssignmentStartMs = System.currentTimeMillis();
        final Map<String, Assignment> firstAssignments = partitionAssignor.assign(clusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        final long firstAssignmentEndMs = System.currentTimeMillis();

        final long firstAssignmentDuration = firstAssignmentEndMs - firstAssignmentStartMs;
        if (firstAssignmentDuration > MAX_ASSIGNMENT_DURATION) {
            throw new AssertionError("The first assignment took too long to complete at " + firstAssignmentDuration + "ms.");
        } else {
            log.info("First assignment took {}ms.", firstAssignmentDuration);
        }

        // Use the assignment to generate the subscriptions' prev task data for the next rebalance
        for (int client = 0; client < numClients; ++client) {
            for (int i = 0; i < numThreadsPerClient; ++i) {
                final String consumer = getConsumerName(i, client);
                final Assignment assignment = firstAssignments.get(consumer);
                final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

                subscriptions.put(
                    consumer,
                    new Subscription(
                        topic,
                        getInfo(uuidForInt(client), new HashSet<>(info.activeTasks()), info.standbyTasks().keySet()).encode(),
                        assignment.partitions())
                );
            }
        }

        final long secondAssignmentStartMs = System.currentTimeMillis();
        final Map<String, Assignment> secondAssignments = partitionAssignor.assign(clusterMetadata, new GroupSubscription(subscriptions)).groupAssignment();
        final long secondAssignmentEndMs = System.currentTimeMillis();
        final long secondAssignmentDuration = secondAssignmentEndMs - secondAssignmentStartMs;
        if (secondAssignmentDuration > MAX_ASSIGNMENT_DURATION) {
            throw new AssertionError("The second assignment took too long to complete at " + secondAssignmentDuration + "ms.");
        } else {
            log.info("Second assignment took {}ms.", secondAssignmentDuration);
        }

        assertThat(secondAssignments.size(), is(numClients * numThreadsPerClient));
    }

    private String getConsumerName(final int consumerIndex, final int clientIndex) {
        return "consumer-" + clientIndex + "-" + consumerIndex;
    }
}
