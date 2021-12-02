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


import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
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
        remoteLogMetadataManagerHarness.setUp();
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
        remoteLogMetadataManagerHarness.createTopic(leaderTopic, JavaConverters.mapAsScalaMap(assignedLeaderTopicReplicas));

        String followerTopic = "follower";
        HashMap<Object, Seq<Object>> assignedFollowerTopicReplicas = new HashMap<>();
        List<Object> followerTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        followerTopicReplicas.add(1);
        followerTopicReplicas.add(2);
        followerTopicReplicas.add(0);
        assignedFollowerTopicReplicas.put(0, JavaConverters.asScalaBuffer(followerTopicReplicas));
        remoteLogMetadataManagerHarness.createTopic(followerTopic, JavaConverters.mapAsScalaMap(assignedFollowerTopicReplicas));

        String topicWithNoMessages = "no-messages-topic";
        HashMap<Object, Seq<Object>> assignedTopicReplicas = new HashMap<>();
        List<Object> noMessagesTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        noMessagesTopicReplicas.add(1);
        noMessagesTopicReplicas.add(2);
        noMessagesTopicReplicas.add(0);
        assignedTopicReplicas.put(0, JavaConverters.asScalaBuffer(noMessagesTopicReplicas));
        remoteLogMetadataManagerHarness.createTopic(topicWithNoMessages, JavaConverters.mapAsScalaMap(assignedTopicReplicas));

        final TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));
        final TopicIdPartition emptyTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));

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
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata));

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
                                                                                        0, 100, -1L, 0,
                                                                                        time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata));

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(leaderTopicIdPartition));
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(followerTopicIdPartition));

        rlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition),
                                                      Collections.emptySet());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        waitUntilConsumerCatchesup(30_000L);
        // leader partitions would have received as it is registered, but follower partition is not yet registered,
        // hence it throws an exception.
        Assertions.assertTrue(rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext());
        Assertions.assertThrows(RemoteStorageException.class, () -> rlmm().listRemoteLogSegments(followerTopicIdPartition));

        // Register follower partition
        rlmm().onPartitionLeadershipChanges(Collections.singleton(emptyTopicIdPartition),
                                                      Collections.singleton(followerTopicIdPartition));

        // Wait until this partition metadata is loaded by the secondary consumer and moved it to primary consumer.
        waitUntilPartitionMovedToPrimary(followerTopicIdPartition, 300_000L);
        // In this state, all the metadata should be available in RLMM for both leader and follower partitions.
        Assertions.assertTrue(rlmm().listRemoteLogSegments(leaderTopicIdPartition).hasNext());
        Assertions.assertTrue(rlmm().listRemoteLogSegments(followerTopicIdPartition).hasNext());
    }

    private void waitUntilPartitionMovedToPrimary(TopicIdPartition topicIdPartition, long timeoutMs) throws TimeoutException {
        long sleepMs = 5000L;
        long time = System.currentTimeMillis();

        while (true) {
            if (System.currentTimeMillis() - time > timeoutMs) {
                throw new TimeoutException("Timed out after " + timeoutMs + "ms ");
            }

            // If both the leader and follower partitions are mapped to the same metadata partition which is 0, it
            // should have at least 2 messages. That means, received offset should be >= 1 (including duplicate messages if any).
            if (rlmm().isUserPartitionAssignedToPrimary(topicIdPartition)) {
                break;
            }

            log.debug("Sleeping for: " + sleepMs);
            Utils.sleep(sleepMs);
        }
    }

    private void waitUntilConsumerCatchesup(long timeoutMs) throws TimeoutException {
        long sleepMs = 5000L;
        long time = System.currentTimeMillis();

        while (true) {
            if (System.currentTimeMillis() - time > timeoutMs) {
                throw new TimeoutException("Timed out after " + timeoutMs + "ms ");
            }

            // If both the leader and follower partitions are mapped to the same metadata partition which is 0, it
            // should have at least 2 messages. That means, received offset should be >= 1 (including duplicate messages if any).
            if (rlmm().receivedOffsetForPartition(0).orElse(-1L) >= 1) {
                break;
            }

            log.debug("Sleeping for: " + sleepMs);
            Utils.sleep(sleepMs);
        }
    }
}
