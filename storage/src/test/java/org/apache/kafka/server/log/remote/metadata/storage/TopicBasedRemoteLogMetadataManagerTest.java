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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicBasedRemoteLogMetadataManagerTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerTest.class);

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private final TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness();
    private RemotePartitionMetadataStore spyRemotePartitionMetadataEventHandler;

    @BeforeEach
    public void setup() {
        // Start the cluster and initialize TopicBasedRemoteLogMetadataManager.
        spyRemotePartitionMetadataEventHandler = spy(new RemotePartitionMetadataStore());
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet(), true, () -> spyRemotePartitionMetadataEventHandler);
    }

    @AfterEach
    public void teardown() throws IOException {
        remoteLogMetadataManagerHarness.close();
    }

    public TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return remoteLogMetadataManagerHarness.remoteLogMetadataManager();
    }

    @Test
    public void testDoesTopicExist() {
        Properties adminConfig = remoteLogMetadataManagerHarness.adminClientConfig();
        ListenerName listenerName = remoteLogMetadataManagerHarness.listenerName();
        try (Admin admin = remoteLogMetadataManagerHarness.createAdminClient(listenerName, adminConfig)) {
            String topic = "test-topic-exist";
            remoteLogMetadataManagerHarness.createTopic(topic, 1, 1, new Properties(),
                    listenerName, adminConfig);
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            Assertions.assertTrue(doesTopicExist);
        }
    }

    @Test
    public void testTopicDoesNotExist() {
        Properties adminConfig = remoteLogMetadataManagerHarness.adminClientConfig();
        ListenerName listenerName = remoteLogMetadataManagerHarness.listenerName();
        try (Admin admin = remoteLogMetadataManagerHarness.createAdminClient(listenerName, adminConfig)) {
            String topic = "dummy-test-topic";
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            Assertions.assertFalse(doesTopicExist);
        }
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
        Assertions.assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(newLeaderTopicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).markInitialized(newFollowerTopicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(leaderSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(followerSegmentMetadata);
        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition).hasNext());
        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition).hasNext());
    }

    @Test
    public void testRemoteLogSizeCalculationForUnknownTopicIdPartitionThrows() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("singleton", 0));
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().remoteLogSize(topicIdPartition, 0));
    }

    @Test
    public void testRemoteLogSizeCalculationWithSegmentsOfTheSameEpoch() throws RemoteStorageException, TimeoutException, InterruptedException {
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
        Assertions.assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(thirdSegmentMetadata);
        Long remoteLogSize = topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 0);

        Assertions.assertEquals(SEG_SIZE * 6, remoteLogSize);
    }

    @Test
    public void testRemoteLogSizeCalculationWithSegmentsOfDifferentEpochs() throws RemoteStorageException, TimeoutException, InterruptedException {
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
        Assertions.assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(thirdSegmentMetadata);
        Assertions.assertEquals(SEG_SIZE, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 0));
        Assertions.assertEquals(SEG_SIZE * 2, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 1));
        Assertions.assertEquals(SEG_SIZE * 3, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 2));
    }

    @Test
    public void testRemoteLogSizeCalculationWithSegmentsHavingNonExistentEpochs() throws RemoteStorageException, TimeoutException, InterruptedException {
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
        Assertions.assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        Assertions.assertEquals(0, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 9001));
    }

}
