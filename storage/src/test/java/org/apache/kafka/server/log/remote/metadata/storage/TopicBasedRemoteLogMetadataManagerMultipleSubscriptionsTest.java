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
package org.apache.kafka.server.log.remote.metadata.storage;


import kafka.utils.EmptyTestInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicBasedRemoteLogMetadataManagerMultipleSubscriptionsTest {

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private final TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness();

    private TopicBasedRemoteLogMetadataManager rlmm() {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager();
    }

    @BeforeEach
    public void setup() {
        // Start the cluster only.
        remoteLogMetadataManagerHarness.setUp(new EmptyTestInfo());
    }

    @AfterEach
    public void teardown() throws IOException {
        remoteLogMetadataManagerHarness.close();
    }

    @Test
    public void testMultiplePartitionSubscriptions() throws Exception {
        // Create topics.
        String leaderTopic = "leader";
        HashMap<Object, Seq<Object>> assignedLeaderTopicReplicas = new HashMap<>();
        List<Object> leaderTopicReplicas = new ArrayList<>();
        // Set broker id 0 as the first entry which is taken as the leader.
        leaderTopicReplicas.add(0);
        leaderTopicReplicas.add(1);
        leaderTopicReplicas.add(2);
        assignedLeaderTopicReplicas.put(0, JavaConverters.asScalaBuffer(leaderTopicReplicas));
        remoteLogMetadataManagerHarness.createTopicWithAssignment(leaderTopic,
            JavaConverters.mapAsScalaMap(assignedLeaderTopicReplicas),
            remoteLogMetadataManagerHarness.listenerName());

        String followerTopic = "follower";
        HashMap<Object, Seq<Object>> assignedFollowerTopicReplicas = new HashMap<>();
        List<Object> followerTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        followerTopicReplicas.add(1);
        followerTopicReplicas.add(2);
        followerTopicReplicas.add(0);
        assignedFollowerTopicReplicas.put(0, JavaConverters.asScalaBuffer(followerTopicReplicas));
        remoteLogMetadataManagerHarness.createTopicWithAssignment(
            followerTopic, JavaConverters.mapAsScalaMap(assignedFollowerTopicReplicas),
            remoteLogMetadataManagerHarness.listenerName());

        String topicWithNoMessages = "no-messages-topic";
        HashMap<Object, Seq<Object>> assignedTopicReplicas = new HashMap<>();
        List<Object> noMessagesTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        noMessagesTopicReplicas.add(1);
        noMessagesTopicReplicas.add(2);
        noMessagesTopicReplicas.add(0);
        assignedTopicReplicas.put(0, JavaConverters.asScalaBuffer(noMessagesTopicReplicas));
        remoteLogMetadataManagerHarness.createTopicWithAssignment(
            topicWithNoMessages, JavaConverters.mapAsScalaMap(assignedTopicReplicas),
            remoteLogMetadataManagerHarness.listenerName());

        final TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));
        final TopicIdPartition emptyTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topicWithNoMessages, 0));

        final RemotePartitionMetadataStore spyRemotePartitionMetadataStore = spy(new RemotePartitionMetadataStore());

        // Think of a Phaser as a CountdownLatch which provides a "countUp" operation in addition to a countDown.
        // The "parties" in a phaser are analogous to the "count". The awaiting semantics of Phaser
        // however differ slightly compared to a CountdownLatch, which requires us to account for
        // the test thread as well while initialising the Phaser.
        Phaser initializationPhaser = new Phaser(2); // 1 to register test thread, 1 to register leaderTopicIdPartition
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            initializationPhaser.arriveAndDeregister(); // similar to CountdownLatch::countDown
            return result;
        }).when(spyRemotePartitionMetadataStore).markInitialized(any());

        Phaser handleRemoteLogSegmentMetadataPhaser = new Phaser(2); // 1 to register test thread, 1 to register leaderTopicIdPartition
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            handleRemoteLogSegmentMetadataPhaser.arriveAndDeregister(); // similar to CountdownLatch::countDown
            return result;
        }).when(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(any());

        remoteLogMetadataManagerHarness.initializeRemoteLogMetadataManager(Collections.emptySet(), true, numMetadataTopicPartitions -> new RemoteLogMetadataTopicPartitioner(numMetadataTopicPartitions) {
            @Override
            public int metadataPartition(TopicIdPartition topicIdPartition) {
                // Always return partition 0 except for noMessagesTopicIdPartition. So that, any new user
                // partition(other than noMessagesTopicIdPartition) added to RLMM will use the same metadata partition.
                // That will make the secondary consumer assignment.
                if (emptyTopicIdPartition.equals(topicIdPartition)) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }, () -> spyRemotePartitionMetadataStore);

        // Add segments for these partitions but an exception is received as they have not yet been subscribed.
        // These messages would have been published to the respective metadata topic partitions but the ConsumerManager
        // has not yet been subscribing as they are not yet registered.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
            0, 100, -1L, 0,
            time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class, () -> rlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata).get());
        Assertions.assertEquals("org.apache.kafka.common.KafkaException: This consumer is not assigned to the target partition 0. Currently assigned partitions: []",
            exception.getMessage());

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
            0, 100, -1L, 0,
            time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        exception = Assertions.assertThrows(ExecutionException.class, () -> rlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata).get());
        Assertions.assertEquals("org.apache.kafka.common.KafkaException: This consumer is not assigned to the target partition 0. Currently assigned partitions: []",
            exception.getMessage());

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(leaderTopicIdPartition));
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(followerTopicIdPartition));

        rlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition),
            Collections.emptySet());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        initializationPhaser.awaitAdvanceInterruptibly(initializationPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS); // similar to CountdownLatch::await
        handleRemoteLogSegmentMetadataPhaser.awaitAdvanceInterruptibly(handleRemoteLogSegmentMetadataPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS);
        verify(spyRemotePartitionMetadataStore).markInitialized(leaderTopicIdPartition);
        verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(leaderSegmentMetadata);
        clearInvocations(spyRemotePartitionMetadataStore);

        // leader partitions would have received as it is registered, but follower partition is not yet registered,
        // hence it throws an exception.
        Assertions.assertTrue(rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext());
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(followerTopicIdPartition));

        // Register follower partition
        // Phaser::bulkRegister and Phaser::register provide the "countUp" feature
        initializationPhaser.bulkRegister(2); // 1 for emptyTopicIdPartition and 1 for followerTopicIdPartition
        handleRemoteLogSegmentMetadataPhaser.register(); // 1 for followerTopicIdPartition, emptyTopicIdPartition doesn't have a RemoteLogSegmentMetadata event
        rlmm().onPartitionLeadershipChanges(Collections.singleton(emptyTopicIdPartition),
            Collections.singleton(followerTopicIdPartition));

        initializationPhaser.awaitAdvanceInterruptibly(initializationPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS);
        handleRemoteLogSegmentMetadataPhaser.awaitAdvanceInterruptibly(handleRemoteLogSegmentMetadataPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS);

        verify(spyRemotePartitionMetadataStore).markInitialized(followerTopicIdPartition);
        verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(followerSegmentMetadata);
        // In this state, all the metadata should be available in RLMM for both leader and follower partitions.
        Assertions.assertTrue(rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext(), "No segments found");
        Assertions.assertTrue(rlmm().listRemoteLogSegments(followerTopicIdPartition).hasNext(), "No segments found");
    }
}
