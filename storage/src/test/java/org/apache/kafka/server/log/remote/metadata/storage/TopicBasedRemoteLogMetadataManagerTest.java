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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("resource")
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
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            clusterInstance.waitTopicCreation(topic, 1);
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            assertTrue(doesTopicExist);
        }
    }

    @ClusterTest
    public void testTopicDoesNotExist() throws ExecutionException, InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            String topic = "dummy-test-topic";
            boolean doesTopicExist = topicBasedRlmm().doesTopicExist(admin, topic);
            assertFalse(doesTopicExist);
        }
    }

    @SuppressWarnings("unchecked")
    @ClusterTest
    public void testDoesTopicExistWithAdminClientExecutionError() throws ExecutionException, InterruptedException {
        // Create a mock Admin client that throws an ExecutionException (not UnknownTopicOrPartitionException)
        Admin mockAdmin = mock(Admin.class);
        DescribeTopicsResult mockDescribeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> mockFuture = mock(KafkaFuture.class);
        
        String topic = "test-topic";
        
        // Set up the mock to throw a RuntimeException wrapped in ExecutionException
        when(mockAdmin.describeTopics(anySet())).thenReturn(mockDescribeTopicsResult);
        when(mockDescribeTopicsResult.topicNameValues()).thenReturn(Map.of(topic, mockFuture));
        when(mockFuture.get()).thenThrow(new ExecutionException("Admin client connection error", new RuntimeException("Connection failed")));
        
        // The method should re-throw the ExecutionException since it's not an UnknownTopicOrPartitionException
        TopicBasedRemoteLogMetadataManager rlmm = topicBasedRlmm();
        assertThrows(ExecutionException.class, () -> rlmm.doesTopicExist(mockAdmin, topic));
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
            admin.createTopics(List.of(new NewTopic(leaderTopic, Map.of(0, List.of(0, 1, 2))))).all().get();
            clusterInstance.waitTopicCreation(leaderTopic, 1);
            admin.createTopics(List.of(new NewTopic(followerTopic, Map.of(0, List.of(1, 2, 0))))).all().get();
            clusterInstance.waitTopicCreation(followerTopic, 1);
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
                                                                                time.milliseconds(), SEG_SIZE, Map.of(0, 0L));
        assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata).get());

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newFollowerTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Map.of(0, 0L));
        assertThrows(Exception.class, () -> topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata).get());

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition));
        assertThrows(RemoteResourceNotFoundException.class, () -> topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition));

        assertFalse(topicBasedRlmm().isReady(newLeaderTopicIdPartition));
        assertFalse(topicBasedRlmm().isReady(newFollowerTopicIdPartition));

        topicBasedRlmm().onPartitionLeadershipChanges(Set.of(newLeaderTopicIdPartition),
                                                      Set.of(newFollowerTopicIdPartition));

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
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Map.of(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Map.of(0, 0L));
        RemoteLogSegmentMetadata thirdSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                200, 300, -1L, 0, time.milliseconds(), SEG_SIZE * 3, Map.of(0, 0L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(thirdSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Set.of(topicIdPartition), Set.of());

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
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Map.of(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Map.of(1, 100L));
        RemoteLogSegmentMetadata thirdSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                200, 300, -1L, 0, time.milliseconds(), SEG_SIZE * 3, Map.of(2, 200L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(thirdSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Set.of(topicIdPartition), Set.of());

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
                0, 100, -1L, 0, time.milliseconds(), SEG_SIZE, Map.of(0, 0L));
        RemoteLogSegmentMetadata secondSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
                100, 200, -1L, 0, time.milliseconds(), SEG_SIZE * 2, Map.of(1, 100L));

        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(firstSegmentMetadata);
        topicBasedRemoteLogMetadataManager.addRemoteLogSegmentMetadata(secondSegmentMetadata);

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(Set.of(topicIdPartition), Set.of());

        // RemoteLogSegmentMetadata events are already published, and topicBasedRlmm's consumer manager will start
        // fetching those events and build the cache.
        assertTrue(initializationLatch.await(30_000, TimeUnit.MILLISECONDS));
        assertTrue(handleRemoteLogSegmentMetadataLatch.await(30_000, TimeUnit.MILLISECONDS));

        verify(spyRemotePartitionMetadataEventHandler).markInitialized(topicIdPartition);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(firstSegmentMetadata);
        verify(spyRemotePartitionMetadataEventHandler).handleRemoteLogSegmentMetadata(secondSegmentMetadata);
        assertEquals(0, topicBasedRemoteLogMetadataManager.remoteLogSize(topicIdPartition, 9001));
    }

    @ClusterTest
    public void testInitializationFailure() throws IOException, InterruptedException {
        // Set up a custom exit procedure for testing
        final AtomicBoolean exitCalled = new AtomicBoolean(false);
        final AtomicInteger exitCode = new AtomicInteger(-1);
        
        // Set custom exit procedure that won't actually exit the process
        Exit.setExitProcedure((statusCode, message) -> {
            exitCalled.set(true);
            exitCode.set(statusCode);
        });

        try (TopicBasedRemoteLogMetadataManager rlmm = new TopicBasedRemoteLogMetadataManager()) {
            // configure rlmm without bootstrap servers, so it will fail to initialize admin client.
            Map<String, Object> configs = Map.of(
                TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR, TestUtils.tempDirectory("rlmm_segs_").getAbsolutePath(),
                TopicBasedRemoteLogMetadataManagerConfig.BROKER_ID, 0
            );
            rlmm.configure(configs);
            rlmm.onBrokerReady();
            
            // Wait for initialization failure and exit procedure to be called
            TestUtils.waitForCondition(() -> exitCalled.get(), 
                "Exit procedure should be called due to initialization failure");
            
            // Verify exit code
            assertEquals(1, exitCode.get(), "Exit code should be 1");
        } finally {
            // Restore default exit procedure
            Exit.resetExitProcedure();
        }
    }

    @ClusterTest
    public void testRemoteLogMetadataTopicWithDefaultMinIsr() throws ExecutionException, InterruptedException {
        // Initialize the manager which will create the __remote_log_metadata topic
        TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager = topicBasedRlmm();
        verifyRemoteLogMetadataTopicWithMinIsr(topicBasedRemoteLogMetadataManager,
                                               TopicBasedRemoteLogMetadataManagerConfig.DEFAULT_REMOTE_LOG_METADATA_TOPIC_MIN_ISR,
                                               "default value");
    }

    @ClusterTest
    public void testRemoteLogMetadataTopicWithCustomMinIsr() throws ExecutionException, InterruptedException, IOException {
        // Create a manager with custom min.isr value
        short customMinIsr = 3;
        Map<String, Object> overrideProps = Map.of(
            TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_MIN_ISR_PROP, customMinIsr
        );
        try (TopicBasedRemoteLogMetadataManager customRlmm = RemoteLogMetadataManagerTestUtils.builder()
                .bootstrapServers(clusterInstance.bootstrapServers())
                .overrideRemoteLogMetadataManagerProps(overrideProps)
                .build()) {
            verifyRemoteLogMetadataTopicWithMinIsr(customRlmm, customMinIsr, "custom value");
        }
    }

    private void verifyRemoteLogMetadataTopicWithMinIsr(TopicBasedRemoteLogMetadataManager rlmm,
                                                        short expectedMinIsr,
                                                        String valueDescription)
                                                        throws ExecutionException, InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            String metadataTopic = TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;
            
            // Wait for the topic to be created
            clusterInstance.waitTopicCreation(metadataTopic, RemoteLogMetadataManagerTestUtils.METADATA_TOPIC_PARTITIONS_COUNT);
            
            // Verify the topic exists
            assertTrue(rlmm.doesTopicExist(admin, metadataTopic));
            
            // Describe the topic configs to verify min.insync.replicas
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, metadataTopic);
            DescribeConfigsResult describeResult = admin.describeConfigs(List.of(topicResource));
            Config config = describeResult.all().get().get(topicResource);

            assertNotNull(config, "Topic config should not be null");
            ConfigEntry minIsrEntry = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
            assertNotNull(minIsrEntry, "min.insync.replicas config should exist");
            assertEquals(String.valueOf(expectedMinIsr), minIsrEntry.value(), 
                "min.insync.replicas should be " + expectedMinIsr + " (" + valueDescription + ")");
        }
    }
}
