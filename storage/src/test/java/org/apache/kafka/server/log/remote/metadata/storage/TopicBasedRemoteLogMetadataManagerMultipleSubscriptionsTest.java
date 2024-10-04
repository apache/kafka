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


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(ClusterTestExtensions.class)
@ClusterTestDefaults(brokers = 3)
public class TopicBasedRemoteLogMetadataManagerMultipleSubscriptionsTest {
    private final ClusterInstance clusterInstance;
    private final Time time = Time.SYSTEM;

    TopicBasedRemoteLogMetadataManagerMultipleSubscriptionsTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testMultiplePartitionSubscriptions() throws Exception {
        // Create topics.
        String leaderTopic = "leader";
        // Set broker id 0 as the first entry which is taken as the leader.
        createTopic(leaderTopic, Collections.singletonMap(0, Arrays.asList(0, 1, 2)));

        String followerTopic = "follower";
        // Set broker id 1 as the first entry which is taken as the leader.
        createTopic(followerTopic, Collections.singletonMap(0, Arrays.asList(1, 2, 0)));

        String topicWithNoMessages = "no-messages-topic";
        // Set broker id 1 as the first entry which is taken as the leader.
        createTopic(topicWithNoMessages, Collections.singletonMap(0, Arrays.asList(1, 2, 0)));

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

        try (TopicBasedRemoteLogMetadataManager remoteLogMetadataManager = RemoteLogMetadataManagerTestUtils.builder()
                .bootstrapServers(clusterInstance.bootstrapServers())
                .startConsumerThread(true)
                .remoteLogMetadataTopicPartitioner(numMetadataTopicPartitions -> new RemoteLogMetadataTopicPartitioner(numMetadataTopicPartitions) {
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
                })
                .remotePartitionMetadataStore(() -> spyRemotePartitionMetadataStore)
                .build()) {

            // Add segments for these partitions but an exception is received as they have not yet been subscribed.
            // These messages would have been published to the respective metadata topic partitions but the ConsumerManager
            // has not yet been subscribing as they are not yet registered.
            int segSize = 1048576;
            RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), segSize, Collections.singletonMap(0, 0L));
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> remoteLogMetadataManager.addRemoteLogSegmentMetadata(leaderSegmentMetadata).get());
            assertEquals("org.apache.kafka.common.KafkaException: This consumer is not assigned to the target partition 0. Currently assigned partitions: []",
                exception.getMessage());

            RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0,
                time.milliseconds(), segSize, Collections.singletonMap(0, 0L));
            exception = assertThrows(ExecutionException.class, () -> remoteLogMetadataManager.addRemoteLogSegmentMetadata(followerSegmentMetadata).get());
            assertEquals("org.apache.kafka.common.KafkaException: This consumer is not assigned to the target partition 0. Currently assigned partitions: []",
                exception.getMessage());

            // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
            assertThrows(RemoteStorageException.class, () -> remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition));
            assertThrows(RemoteStorageException.class, () -> remoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition));

            remoteLogMetadataManager.onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition),
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
            assertTrue(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition).hasNext());
            assertThrows(RemoteStorageException.class, () -> remoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition));

            // Register follower partition
            // Phaser::bulkRegister and Phaser::register provide the "countUp" feature
            initializationPhaser.bulkRegister(2); // 1 for emptyTopicIdPartition and 1 for followerTopicIdPartition
            handleRemoteLogSegmentMetadataPhaser.register(); // 1 for followerTopicIdPartition, emptyTopicIdPartition doesn't have a RemoteLogSegmentMetadata event
            remoteLogMetadataManager.onPartitionLeadershipChanges(Collections.singleton(emptyTopicIdPartition),
                    Collections.singleton(followerTopicIdPartition));

            initializationPhaser.awaitAdvanceInterruptibly(initializationPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS);
            handleRemoteLogSegmentMetadataPhaser.awaitAdvanceInterruptibly(handleRemoteLogSegmentMetadataPhaser.arrive(), 30_000, TimeUnit.MILLISECONDS);

            verify(spyRemotePartitionMetadataStore).markInitialized(followerTopicIdPartition);
            verify(spyRemotePartitionMetadataStore).handleRemoteLogSegmentMetadata(followerSegmentMetadata);
            // In this state, all the metadata should be available in RLMM for both leader and follower partitions.
            assertTrue(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition).hasNext(), "No segments found");
            assertTrue(remoteLogMetadataManager.listRemoteLogSegments(followerTopicIdPartition).hasNext(), "No segments found");
        }
    }

    private void createTopic(String topic, Map<Integer, List<Integer>> replicasAssignments) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, replicasAssignments)));
            assertDoesNotThrow(() -> clusterInstance.waitForTopic(topic, replicasAssignments.size()));
        }
    }
}
