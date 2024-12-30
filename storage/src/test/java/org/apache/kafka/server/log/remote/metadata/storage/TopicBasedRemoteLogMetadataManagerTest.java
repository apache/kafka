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
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(ClusterTestExtensions.class)
@ClusterTestDefaults(brokers = 3)
public class TopicBasedRemoteLogMetadataManagerTest {
    private static final int SEG_SIZE = 1048576;
    private final ClusterInstance clusterInstance;
    private final RemotePartitionMetadataStore spyRemotePartitionMetadataEventHandler = spy(new RemotePartitionMetadataStore());
    private final Time time = Time.SYSTEM;
    private TopicBasedRemoteLogMetadataManager remoteLogMetadataManager;

    TopicBasedRemoteLogMetadataManagerTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        if (remoteLogMetadataManager == null)
            remoteLogMetadataManager = RemoteLogMetadataManagerTestUtils.builder()
                .bootstrapServers(clusterInstance.bootstrapServers())
                .startConsumerThread(true)
                .remotePartitionMetadataStore(() -> spyRemotePartitionMetadataEventHandler)
                .build();
        return remoteLogMetadataManager;
    }

    @AfterEach
    public void teardown() throws IOException {
        if (remoteLogMetadataManager != null) remoteLogMetadataManager.close();
    }

    @ClusterTest
    public void testDoesTopicExist() throws ExecutionException, InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            String topic = "test-topic-exist";
            admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
            clusterInstance.waitForTopic(topic, 1);
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            assertTrue(doesTopicExist);
        }
    }

    @ClusterTest
    public void testTopicDoesNotExist() {
        try (Admin admin = clusterInstance.admin()) {
            String topic = "dummy-test-topic";
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            assertFalse(doesTopicExist);
        }
    }

    @ClusterTest
    public void testWithNoAssignedPartitions() {
        // This test checks simple lifecycle of TopicBasedRemoteLogMetadataManager with out assigning any leader/follower partitions.
        // This should close successfully releasing the resources.
        topicBasedRlmm();
    }

    @ClusterTest
    public void testNewPartitionUpdates() throws Exception {
        // Create topics.
        String leaderTopic = "new-leader";
        String followerTopic = "new-follower";
        try (Admin admin = clusterInstance.admin()) {
            // Set broker id 0 as the first entry which is taken as the leader.
            admin.createTopics(Collections.singletonList(new NewTopic(leaderTopic, Collections.singletonMap(0, Arrays.asList(0, 1, 2))))).all().get();
            clusterInstance.waitForTopic(leaderTopic, 1);
            admin.createTopics(Collections.singletonList(new NewTopic(followerTopic, Collections.singletonMap(0, Arrays.asList(1, 2, 0))))).all().get();
            clusterInstance.waitForTopic(followerTopic, 1);
        }

        final TopicIdPartition newLeaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition newFollowerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));

        CountDownLatch initializationLatch = new CountDownLatch(2);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            initializationLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).markInitialized(any());

        CountDownLatch handleRemoteLogSegmentMetadataLatch = new CountDownLatch(2);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            handleRemoteLogSegmentMetadataLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(any());

        // Add segments for these partitions but an exception is received as they have not yet been subscribed.
        // These messages would have been published to the respective metadata topic partitions but the ConsumerManager
        // has not yet been subscribing as they are not yet registered.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newLeaderTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata).get());

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newFollowerTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata).get());

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition));
        assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition));

        assertFalse(topicBasedRlmm().isReady(newLeaderTopicIdPartition));
        assertFalse(topicBasedRlmm().isReady(newFollowerTopicIdPartition));

        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(newLeaderTopicIdPartition),
                                                      Collections.singleton(newFollowerTopicIdPartition));

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(newLeaderTopicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).markInitialized(newFollowerTopicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(leaderSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(followerSegmentMetadata);
        assertTrue(topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition).hasNext());
        assertTrue(topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition).hasNext());

        assertTrue(topicBasedRlmm().isReady(newLeaderTopicIdPartition));
        assertTrue(topicBasedRlmm().isReady(newFollowerTopicIdPartition));
    }

    @ClusterTest
    public void testRemoteLogSizeCalculationForUnknownTopicIdPartitionThrows() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("singleton", 0));
        assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().remoteLogSize(topicIdPartition, 0));
    }

    @ClusterTest
    public void testRemoteLogSizeCalculationWithSegmentsOfTheSameEpoch() throws RemoteStorageException, InterruptedException {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("singleton", 0));
        TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = topicBasedRlmm();

        CountDownLatch initializationLatch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            initializationLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).markInitialized(any());

        CountDownLatch handleRemoteLogSegmentMetadataLatch = new CountDownLatch(3);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            handleRemoteLogSegmentMetadataLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(any());

        RemoteLogSegmentMetadata firstSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Collections.singletonMap(0, 0L));
        RemoteLogSegmentMetadata thirdSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                200, 300, -1L, 0, time.milliseconds(), SEG_SIZE * 3, Collections.singletonMap(0, 0L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(thirdSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(thirdSegmentMetadata);
        Long remoteLogSize = topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 0);

        assertEquals(SEG_SIZE * 6, remoteLogSize);
    }

    @ClusterTest
    public void testRemoteLogSizeCalculationWithSegmentsOfDifferentEpochs() throws RemoteStorageException, InterruptedException {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("singleton", 0));
        TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = topicBasedRlmm();
        CountDownLatch initializationLatch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            initializationLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).markInitialized(any());

        CountDownLatch handleRemoteLogSegmentMetadataLatch = new CountDownLatch(3);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            handleRemoteLogSegmentMetadataLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(any());

        RemoteLogSegmentMetadata firstSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Collections.singletonMap(1, 100L));
        RemoteLogSegmentMetadata thirdSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                200, 300, -1L, 0, time.milliseconds(), SEG_SIZE * 3, Collections.singletonMap(2, 200L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(thirdSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(thirdSegmentMetadata);
        assertEquals(SEG_SIZE, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 0));
        assertEquals(SEG_SIZE * 2, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 1));
        assertEquals(SEG_SIZE * 3, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 2));
    }

    @ClusterTest
    public void testRemoteLogSizeCalculationWithSegmentsHavingNonExistentEpochs() throws RemoteStorageException, InterruptedException {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("singleton", 0));
        TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = topicBasedRlmm();
        CountDownLatch initializationLatch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            initializationLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).markInitialized(any());

        CountDownLatch handleRemoteLogSegmentMetadataLatch = new CountDownLatch(2);
        doAnswer(invocationOnMock -> {
            Object result = invocationOnMock.callRealMethod();
            handleRemoteLogSegmentMetadataLatch.countDown();
            return result;
        }).when(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(any());

        RemoteLogSegmentMetadata firstSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Collections.singletonMap(1, 100L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Collections.singleton(topicIdPartition), Collections.emptySet());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        assertEquals(0, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 9001));
    }

}
