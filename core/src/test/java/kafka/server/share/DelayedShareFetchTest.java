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

import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class DelayedShareFetchTest {
    private static final int MAX_WAIT_MS = 5000;

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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);

        // Since there is no partition that can be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsTrue() {
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, replicaManager, partitionCacheMap);
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since no partition could be acquired, the future should be empty and replicaManager.readFromLog should not be called.
        assertEquals(0, shareFetchPartitionData.future().join().size());
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, replicaManager, partitionCacheMap);
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                future, partitionMaxBytes);

        DelayedShareFetch delayedShareFetch = spy(new DelayedShareFetch(shareFetchPartitionData, replicaManager, partitionCacheMap));
        assertFalse(delayedShareFetch.isCompleted());

        // Completing the future before calling forceComplete which can happen in a real world scenario where the future
        // might be completed by another thread which has the same share fetch request entry.
        future.complete(Collections.emptyMap());
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that forceComplete does not call topicPartitionDataForAcquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(0)).acquirablePartitions();
    }
}
