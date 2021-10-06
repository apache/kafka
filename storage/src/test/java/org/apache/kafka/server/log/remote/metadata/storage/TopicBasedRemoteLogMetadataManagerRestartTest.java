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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.server.log.remote.metadata.storage.ConsumerManager.COMMITTED_OFFSETS_FILE_NAME;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicBasedRemoteLogMetadataManagerRestartTest {

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private final String logDir = TestUtils.tempDirectory("_rlmm_segs_").getAbsolutePath();

    private TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness;

    @BeforeEach
    public void setup() {
        // Start the cluster and initialize TopicBasedRemoteLogMetadataManager.
        remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness() {
            protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
                Map<String, Object> props = new HashMap<>();
                props.put(LOG_DIR, logDir);
                return props;
            }
        };
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet(), true);
    }

    private void startTopicBasedRemoteLogMetadataManagerHarness(boolean startConsumerThread) {
        remoteLogMetadataManagerHarness.initializeRemoteLogMetadataManager(Collections.emptySet(), startConsumerThread);
    }

    @AfterEach
    public void teardown() throws IOException {
        if (remoteLogMetadataManagerHarness != null) {
            remoteLogMetadataManagerHarness.close();
        }
    }

    private void stopTopicBasedRemoteLogMetadataManagerHarness() throws IOException {
        remoteLogMetadataManagerHarness.closeRemoteLogMetadataManager();
    }

    private TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager();
    }

    @Test
    public void testRLMMAPIsAfterRestart() throws Exception {
        // Create topics.
        String leaderTopic = "new-leader";
        HashMap<Object, Seq<Object>> assignedLeaderTopicReplicas = new HashMap<>();
        List<Object> leaderTopicReplicas = new ArrayList<>();
        // Set broker id 0 as the first entry which is taken as the leader.
        leaderTopicReplicas.add(0);
        leaderTopicReplicas.add(1);
        leaderTopicReplicas.add(2);
        assignedLeaderTopicReplicas.put(0, JavaConverters.asScalaBuffer(leaderTopicReplicas));
        remoteLogMetadataManagerHarness.createTopic(leaderTopic, JavaConverters.mapAsScalaMap(assignedLeaderTopicReplicas));

        String followerTopic = "new-follower";
        HashMap<Object, Seq<Object>> assignedFollowerTopicReplicas = new HashMap<>();
        List<Object> followerTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        followerTopicReplicas.add(1);
        followerTopicReplicas.add(2);
        followerTopicReplicas.add(0);
        assignedFollowerTopicReplicas.put(0, JavaConverters.asScalaBuffer(followerTopicReplicas));
        remoteLogMetadataManagerHarness.createTopic(followerTopic, JavaConverters.mapAsScalaMap(assignedFollowerTopicReplicas));

        final TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));

        // Register these partitions to RLMM.
        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

        // Add segments for these partitions but they are not available as they have not yet been subscribed.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata).get();

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata).get();

        // Stop TopicBasedRemoteLogMetadataManager only.
        stopTopicBasedRemoteLogMetadataManagerHarness();

        // Start TopicBasedRemoteLogMetadataManager but do not start consumer thread to check whether the stored metadata is
        // loaded successfully or not.
        startTopicBasedRemoteLogMetadataManagerHarness(false);

        // Register these partitions to RLMM, which loads the respective metadata snapshots.
        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

        // Check for the stored entries from the earlier run.
        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(Collections.singleton(leaderSegmentMetadata).iterator(),
                                                                 topicBasedRlmm().listRemoteLogSegments(leaderTopicIdPartition)));
        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(Collections.singleton(followerSegmentMetadata).iterator(),
                                                                 topicBasedRlmm().listRemoteLogSegments(followerTopicIdPartition)));
        // Check whether the check-pointed consumer offsets are stored or not.
        Path committedOffsetsPath = new File(logDir, COMMITTED_OFFSETS_FILE_NAME).toPath();
        Assertions.assertTrue(committedOffsetsPath.toFile().exists());
        CommittedOffsetsFile committedOffsetsFile = new CommittedOffsetsFile(committedOffsetsPath.toFile());

        int metadataPartition1 = topicBasedRlmm().metadataPartition(leaderTopicIdPartition);
        int metadataPartition2 = topicBasedRlmm().metadataPartition(followerTopicIdPartition);
        Optional<Long> receivedOffsetForPartition1 = topicBasedRlmm().receivedOffsetForPartition(metadataPartition1);
        Optional<Long> receivedOffsetForPartition2 = topicBasedRlmm().receivedOffsetForPartition(metadataPartition2);
        Assertions.assertTrue(receivedOffsetForPartition1.isPresent());
        Assertions.assertTrue(receivedOffsetForPartition2.isPresent());

        // Make sure these offsets are at least 0.
        Assertions.assertTrue(receivedOffsetForPartition1.get() >= 0);
        Assertions.assertTrue(receivedOffsetForPartition2.get() >= 0);

        // Check the stored entries and the offsets that were set on consumer are the same.
        Map<Integer, Long> partitionToOffset = committedOffsetsFile.readEntries();
        Assertions.assertEquals(partitionToOffset.get(metadataPartition1), receivedOffsetForPartition1.get());
        Assertions.assertEquals(partitionToOffset.get(metadataPartition2), receivedOffsetForPartition2.get());

        // Start Consumer thread
        topicBasedRlmm().startConsumerThread();

        // Add one more segment
        RemoteLogSegmentMetadata leaderSegmentMetadata2 = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                101, 200, -1L, 0,
                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 101L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata2).get();

        // Check that both the stored segment and recently added segment are available.
        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(Arrays.asList(leaderSegmentMetadata, leaderSegmentMetadata2).iterator(),
                                                                 topicBasedRlmm().listRemoteLogSegments(leaderTopicIdPartition)));
    }

}