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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class TopicBasedRemoteLogMetadataManagerRestartTest {

    private final Time time = Time.SYSTEM;
    private final String logDir = TestUtils.tempDirectory("_rlmm_segs_").getAbsolutePath();
    private final ClusterInstance clusterInstance;

    TopicBasedRemoteLogMetadataManagerRestartTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    private TopicBasedRemoteLogMetadataManager createTopicBasedRemoteLogMetadataManager() {
        return RemoteLogMetadataManagerTestUtils.builder()
                .bootstrapServers(clusterInstance.bootstrapServers())
                .startConsumerThread(true)
                .remoteLogMetadataTopicPartitioner(RemoteLogMetadataTopicPartitioner::new)
                .overrideRemoteLogMetadataManagerProps(Collections.singletonMap(LOG_DIR, logDir))
                .build();
    }

    @ClusterTest(brokers = 3)
    public void testRLMMAPIsAfterRestart() throws Exception {
        // Create topics.
        String leaderTopic = "new-leader";
        String followerTopic = "new-follower";
        try (Admin admin = clusterInstance.admin()) {
            // Set broker id 0 as the first entry which is taken as the leader.
            NewTopic newLeaderTopic = new NewTopic(leaderTopic, Collections.singletonMap(0, Arrays.asList(0, 1, 2)));
            // Set broker id 1 as the first entry which is taken as the leader.
            NewTopic newFollowerTopic = new NewTopic(followerTopic, Collections.singletonMap(0, Arrays.asList(1, 2, 0)));
            admin.createTopics(Arrays.asList(newLeaderTopic, newFollowerTopic)).all().get();
        }
        clusterInstance.waitForTopic(leaderTopic, 1);
        clusterInstance.waitForTopic(followerTopic, 1);

        TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));
        int segSize = 1048576;
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), segSize, Collections.singletonMap(0, 0L));
        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), segSize, Collections.singletonMap(0, 0L));

        try (TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = createTopicBasedRemoteLogMetadataManager()) {
            // Register these partitions to RemoteLogMetadataManager.
            topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(
                    Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

            // Add segments for these partitions, but they are not available as they have not yet been subscribed.
            topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(leaderSegmentMetadata).get();
            topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(followerSegmentMetadata).get();
        }

        try (TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = createTopicBasedRemoteLogMetadataManager()) {
            // Register these partitions to RemoteLogMetadataManager, which loads the respective metadata snapshots.
            topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(
                    Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

            // Check for the stored entries from the earlier run.
            TestUtils.waitForCondition(() ->
                            TestUtils.sameElementsWithoutOrder(Collections.singleton(leaderSegmentMetadata).iterator(),
                                    topicBasedRemoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition)),
                    "Remote log segment metadata not available");
            TestUtils.waitForCondition(() ->
                            TestUtils.sameElementsWithoutOrder(Collections.singleton(followerSegmentMetadata).iterator(),
                                    topicBasedRemoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition)),
                    "Remote log segment metadata not available");
            // Add one more segment
            RemoteLogSegmentMetadata leaderSegmentMetadata2 = new RemoteLogSegmentMetadata(
                    new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                    101, 200, -1L, 0,
                    time.milliseconds(), segSize, Collections.singletonMap(0, 101L));
            topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(leaderSegmentMetadata2).get();

            // Check that both the stored segment and recently added segment are available.
            assertTrue(TestUtils.sameElementsWithoutOrder(
                    Arrays.asList(leaderSegmentMetadata, leaderSegmentMetadata2).iterator(),
                    topicBasedRemoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition))
            );
        }
    }
}
