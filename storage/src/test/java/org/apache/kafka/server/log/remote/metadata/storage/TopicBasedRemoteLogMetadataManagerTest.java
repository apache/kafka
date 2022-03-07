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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
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
import java.util.concurrent.TimeoutException;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicBasedRemoteLogMetadataManagerTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerTest.class);

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private final TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness();

    @BeforeEach
    public void setup() {
        // Start the cluster and initialize TopicBasedRemoteLogMetadataManager.
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet(), true);
    }

    @AfterEach
    public void teardown() throws IOException {
        remoteLogMetadataManagerHarness.close();
    }

    public TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager();
    }

    @Test
    public void testWithNoAssignedPartitions() throws Exception {
        // This test checks simple lifecycle of TopicBasedRemoteLogMetadataManager with out assigning any leader/follower partitions.
        // This should close successfully releasing the resources.
        log.info("Not assigning any partitions on TopicBasedRemoteLogMetadataManager");
    }

    @Test
    public void testNewPartitionUpdates() throws Exception {
        // Create topics.
        String leaderTopic = "new-leader";
        HashMap<Object, Seq<Object>> assignedLeaderTopicReplicas = new HashMap<>();
        List<Object> leaderTopicReplicas = new ArrayList<>();
        // Set broker id 0 as the first entry which is taken as the leader.
        leaderTopicReplicas.add(0);
        leaderTopicReplicas.add(1);
        leaderTopicReplicas.add(2);
        assignedLeaderTopicReplicas.put(0, JavaConverters.asScalaBuffer(leaderTopicReplicas));
        remoteLogMetadataManagerHarness.createTopicWithAssignment(
            leaderTopic, JavaConverters.mapAsScalaMap(assignedLeaderTopicReplicas),
            remoteLogMetadataManagerHarness.listenerName());

        String followerTopic = "new-follower";
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

        final TopicIdPartition newLeaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition newFollowerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));

        // Add segments for these partitions but an exception is received as they have not yet been subscribed.
        // These messages would have been published to the respective metadata topic partitions but the ConsumerManager
        // has not yet been subscribing as they are not yet registered.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newLeaderTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        Assertions.assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata).get());

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newFollowerTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        Assertions.assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata).get());

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition));
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition));

        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(newLeaderTopicIdPartition),
                                                      Collections.singleton(newFollowerTopicIdPartition));

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        waitUntilConsumerCatchesup(newLeaderTopicIdPartition, newFollowerTopicIdPartition, 30_000L);

        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition).hasNext());
        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition).hasNext());
    }

    private void waitUntilConsumerCatchesup(TopicIdPartition newLeaderTopicIdPartition,
                                          TopicIdPartition newFollowerTopicIdPartition,
                                          long timeoutMs) throws TimeoutException {
        int leaderMetadataPartition = topicBasedRlmm().metadataPartition(newLeaderTopicIdPartition);
        int followerMetadataPartition = topicBasedRlmm().metadataPartition(newFollowerTopicIdPartition);

        log.debug("Metadata partition for newLeaderTopicIdPartition: [{}], is: [{}]", newLeaderTopicIdPartition, leaderMetadataPartition);
        log.debug("Metadata partition for newFollowerTopicIdPartition: [{}], is: [{}]", newFollowerTopicIdPartition, followerMetadataPartition);

        long sleepMs = 100L;
        long time = System.currentTimeMillis();

        while (true) {
            if (System.currentTimeMillis() - time > timeoutMs) {
                throw new TimeoutException("Timed out after " + timeoutMs + "ms ");
            }

            // If both the leader and follower partitions are mapped to the same metadata partition then it should have at least
            // 2 messages. That means, received offset should be >= 1 (including duplicate messages if any).
            if (leaderMetadataPartition == followerMetadataPartition) {
                if (topicBasedRlmm().receivedOffsetForPartition(leaderMetadataPartition).orElse(-1L) >= 1) {
                    break;
                }
            } else {
                // If the leader partition and the follower partition are mapped to different metadata partitions then
                // each of those metadata partitions will have at least 1 message. That means, received offset should
                // be >= 0 (including duplicate messages if any).
                if (topicBasedRlmm().receivedOffsetForPartition(leaderMetadataPartition).orElse(-1L) >= 0 ||
                        topicBasedRlmm().receivedOffsetForPartition(followerMetadataPartition).orElse(-1L) >= 0) {
                    break;
                }
            }

            log.debug("Sleeping for: " + sleepMs);
            Utils.sleep(sleepMs);
        }
    }

}
