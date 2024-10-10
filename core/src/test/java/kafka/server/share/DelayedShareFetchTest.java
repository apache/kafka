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
package kafka.server.share;

import kafka.server.DelayedActionQueue;
import kafka.server.DelayedOperationPurgatory;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import scala.jdk.javaapi.CollectionConverters;

import static kafka.server.share.SharePartitionManagerTest.DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL;
import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static kafka.server.share.SharePartitionManagerTest.buildLogReadResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class DelayedShareFetchTest {
    private static final int MAX_WAIT_MS = 5000;
    private static Timer mockTimer;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("DelayedShareFetchTestReaper",
            new SystemTimer("DelayedShareFetchTestTimer"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsFalse() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData)
            .withSharePartitionManager(sharePartitionManager)
            .build();

        // Since there is no partition that can be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsTrue() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(any(), any())).thenReturn(
            Collections.singletonList(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData)
            .withSharePartitionManager(sharePartitionManager)
            .withReplicaManager(replicaManager)
            .build();
        assertFalse(delayedShareFetch.isCompleted());

        // Since sp1 can be acquired, tryComplete should return true.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
    }

    @Test
    public void testEmptyFutureReturnedByDelayedShareFetchOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData)
            .withReplicaManager(replicaManager)
            .withSharePartitionManager(sharePartitionManager)
            .build();
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since no partition could be acquired, the future should be empty and replicaManager.readFromLog should not be called.
        assertEquals(0, shareFetchData.future().join().size());
        Mockito.verify(replicaManager, times(0)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertTrue(delayedShareFetch.isCompleted());
    }

    @Test
    public void testReplicaManagerFetchShouldHappenOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(any(), any())).thenReturn(
            Collections.singletonList(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData)
            .withReplicaManager(replicaManager)
            .withSharePartitionManager(sharePartitionManager)
            .build();
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since we can acquire records from sp0, replicaManager.readFromLog should be called once and only for sp0.
        Mockito.verify(replicaManager, times(1)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(sp0, times(1)).nextFetchOffset();
        Mockito.verify(sp1, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.isCompleted());
    }

    @Test
    public void testToCompleteAnAlreadyCompletedFuture() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                future, partitionMaxBytes);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(false);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData)
            .withReplicaManager(replicaManager)
            .withSharePartitionManager(sharePartitionManager)
            .build());
        assertFalse(delayedShareFetch.isCompleted());

        // Force completing the share fetch request for the first time should complete the future with an empty map.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the first forceComplete calls acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions();
        assertEquals(0, shareFetchData.future().join().size());

        // Force completing the share fetch request for the second time should hit the future completion check and not
        // proceed ahead in the function.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the second forceComplete does not call acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions();
    }

    @Test
    public void testForceCompleteTriggersDelayedActionsQueue() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicId, new TopicPartition("foo", 2));
        Map<TopicIdPartition, Integer> partitionMaxBytes1 = new HashMap<>();
        partitionMaxBytes1.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes1.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        // No share partition is available for acquiring initially.
        when(sp0.maybeAcquireFetchLock()).thenReturn(false);
        when(sp1.maybeAcquireFetchLock()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(false);

        SharePartitionManager sharePartitionManager1 = mock(SharePartitionManager.class);
        when(sharePartitionManager1.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager1.sharePartition(groupId, tp1)).thenReturn(sp1);
        when(sharePartitionManager1.sharePartition(groupId, tp2)).thenReturn(sp2);

        ShareFetchData shareFetchData1 = new ShareFetchData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes1);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);

        Set<Object> delayedShareFetchWatchKeys = new HashSet<>();
        partitionMaxBytes1.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        DelayedShareFetch delayedShareFetch1 = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData1)
            .withReplicaManager(replicaManager)
            .withSharePartitionManager(sharePartitionManager1)
            .build();

        // We add a delayed share fetch entry to the purgatory which will be waiting for completion since neither of the
        // partitions in the share fetch request can be acquired.
        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch1, CollectionConverters.asScala(delayedShareFetchWatchKeys).toSeq());

        assertEquals(2, delayedShareFetchPurgatory.watched());
        assertFalse(shareFetchData1.future().isDone());

        Map<TopicIdPartition, Integer> partitionMaxBytes2 = new HashMap<>();
        partitionMaxBytes2.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes2.put(tp2, PARTITION_MAX_BYTES);
        ShareFetchData shareFetchData2 = new ShareFetchData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes2);

        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        DelayedActionQueue delayedActionQueue = spy(new DelayedActionQueue());

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);
        SharePartitionManager sharePartitionManager2 = SharePartitionManagerTest.SharePartitionManagerBuilder
            .builder()
            .withDelayedShareFetchPurgatory(delayedShareFetchPurgatory)
            .withDelayedActionsQueue(delayedActionQueue)
            .withPartitionCacheMap(partitionCacheMap)
            .build();

        DelayedShareFetch delayedShareFetch2 = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetchData2)
            .withReplicaManager(replicaManager)
            .withSharePartitionManager(sharePartitionManager2)
            .build();

        // sp1 can be acquired now
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp1.acquire(any(), any())).thenReturn(
            Collections.singletonList(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));

        // when forceComplete is called for delayedShareFetch2, since tp1 is common in between delayed share fetch
        // requests, it should add a "check and complete" action for request key tp1 on the purgatory.
        delayedShareFetch2.forceComplete();
        assertTrue(delayedShareFetch2.isCompleted());
        assertTrue(shareFetchData2.future().isDone());
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertFalse(delayedShareFetch1.isCompleted());
        Mockito.verify(delayedActionQueue, times(1)).add(any());
        Mockito.verify(delayedActionQueue, times(0)).tryCompleteActions();
    }

    static class DelayedShareFetchBuilder {
        ShareFetchData shareFetchData = mock(ShareFetchData.class);
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);

        DelayedShareFetchBuilder withShareFetchData(ShareFetchData shareFetchData) {
            this.shareFetchData = shareFetchData;
            return this;
        }

        DelayedShareFetchBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        DelayedShareFetchBuilder withSharePartitionManager(SharePartitionManager sharePartitionManager) {
            this.sharePartitionManager = sharePartitionManager;
            return this;
        }

        public static DelayedShareFetchBuilder builder() {
            return new DelayedShareFetchBuilder();
        }

        public DelayedShareFetch build() {
            return new DelayedShareFetch(
                shareFetchData,
                replicaManager,
                sharePartitionManager);
        }
    }
}
