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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicBasedRemoteLogMetadataManagerMultipleSubscriptionsTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerMultipleSubscriptionsTest.class);

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

        RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(10) {
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
        };

        remoteLogMetadataManagerHarness.initializeRemoteLogMetadataManager(Collections.emptySet(), true, partitioner);

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
        waitUntilConsumerCatchesUp(30_000L);
        // leader partitions would have received as it is registered, but follower partition is not yet registered,
        // hence it throws an exception.
        Assertions.assertTrue(rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext());
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(followerTopicIdPartition));

        // Register follower partition
        rlmm().onPartitionLeadershipChanges(Collections.singleton(emptyTopicIdPartition),
            Collections.singleton(followerTopicIdPartition));

        // In this state, all the metadata should be available in RLMM for both leader and follower partitions.
        TestUtils.waitForCondition(() -> rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext(), "No segments found");
        TestUtils.waitForCondition(() -> rlmm().listRemoteLogSegments(followerTopicIdPartition).hasNext(), "No segments found");
    }

    private void waitUntilConsumerCatchesUp(long timeoutMs) throws TimeoutException, InterruptedException {
        TestUtils.waitForCondition(() -> {
            // If both the leader and follower partitions are mapped to the same metadata partition which is 0, it
            // should have at least 2 messages. That means, read offset should be >= 1 (including duplicate messages if any).
            return rlmm().readOffsetForPartition(0).orElse(-1L) >= 1;
        }, timeoutMs, "Consumer did not catch up");
    }
}
