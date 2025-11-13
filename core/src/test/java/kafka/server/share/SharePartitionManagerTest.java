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

import kafka.cluster.Partition;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidShareSessionEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.ShareSessionLimitReachedException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.purgatory.DelayedOperationKey;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ErroneousAndValidPartitionData;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.context.FinalContext;
import org.apache.kafka.server.share.context.ShareFetchContext;
import org.apache.kafka.server.share.context.ShareSessionContext;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.PartitionMaxBytesStrategy;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.session.ShareSession;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.share.session.ShareSessionKey;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static kafka.server.share.DelayedShareFetchTest.mockTopicIdPartitionToReturnDataEqualToMinBytes;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.validateRotatedListEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(120)
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class SharePartitionManagerTest {

    private static final int DEFAULT_RECORD_LOCK_DURATION_MS = 30000;
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    private static final byte BATCH_OPTIMIZED = ShareAcquireMode.BATCH_OPTIMIZED.id();
    private static final short MAX_FETCH_RECORDS = 500;
    private static final int DELAYED_SHARE_FETCH_MAX_WAIT_MS = 2000;
    private static final int DELAYED_SHARE_FETCH_TIMEOUT_MS = 3000;
    private static final int BATCH_SIZE = 500;
    private static final FetchParams FETCH_PARAMS = new FetchParams(
        FetchRequest.ORDINARY_CONSUMER_ID, -1, DELAYED_SHARE_FETCH_MAX_WAIT_MS,
        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty(), true);
    private static final String TIMER_NAME_PREFIX = "share-partition-manager";
    private static final String CONNECTION_ID = "id-1";

    static final int DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL = 1000;
    static final long REMOTE_FETCH_MAX_WAIT_MS = 6000L;

    private MockTime time;
    private ReplicaManager mockReplicaManager;
    private BrokerTopicStats brokerTopicStats;
    private SharePartitionManager sharePartitionManager;

    private static final List<TopicIdPartition> EMPTY_PART_LIST = List.of();
    private static final List<ShareFetchResponseData.AcquiredRecords> EMPTY_ACQUIRED_RECORDS = List.of();

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        kafka.utils.TestUtils.clearYammerMetrics();
        brokerTopicStats = new BrokerTopicStats();
        mockReplicaManager = mock(ReplicaManager.class);
        Partition partition = mockPartition();
        when(mockReplicaManager.getPartitionOrException((TopicPartition) any())).thenReturn(partition);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (sharePartitionManager != null) {
            sharePartitionManager.close();
        }
        brokerTopicStats.close();
        assertNoReaperThreadsPendingClose();
    }

    @Test
    public void testNewContextReturnsFinalContextWithoutRequestData() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData1 = List.of(tp0, tp1);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST, memberId, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context2.getClass());
    }

    @Test
    public void testNewContextReturnsFinalContextWithRequestData() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData1 = List.of(tp0, tp1);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // Sending a Request with FINAL_EPOCH. This should return a FinalContext.
        List<TopicIdPartition> reqData2 = List.of(tp0, tp1);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context2.getClass());
    }

    @Test
    public void testNewContextReturnsFinalContextWhenTopicPartitionsArePresentInRequestData() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData1 = List.of(tp0, tp1);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // shareFetch is not empty, and it contains tpId1, which should return FinalContext instance since it is FINAL_EPOCH
        List<TopicIdPartition> reqData2 = List.of(new TopicIdPartition(tpId1, new TopicPartition("foo", 0)));
        assertInstanceOf(FinalContext.class,
            sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID));
    }

    @Test
    public void testNewContextThrowsErrorWhenShareSessionNotFoundOnFinalEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext("grp", EMPTY_PART_LIST, EMPTY_PART_LIST,
            Uuid.randomUuid().toString(), ShareRequestMetadata.FINAL_EPOCH, false, CONNECTION_ID));
    }

    @Test
    public void testNewContextThrowsErrorWhenAcknowledgeDataPresentOnInitialEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();
        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        assertThrows(InvalidRequestException.class, () -> sharePartitionManager.newContext("grp", List.of(tp0, tp1), EMPTY_PART_LIST,
            Uuid.randomUuid().toString(), ShareRequestMetadata.INITIAL_EPOCH, true, CONNECTION_ID));
    }

    @Test
    public void testNewContextThrowsErrorWhenShareSessionCacheIsFullOnInitialEpoch() {
        // Define a cache with max size 1
        ShareSessionCache cache = new ShareSessionCache(1);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData = List.of(tp0, tp1);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // Trying to create a new share session, but since cache is already full, it should throw an exception
        assertThrows(ShareSessionLimitReachedException.class, () -> sharePartitionManager.newContext("grp", reqData, EMPTY_PART_LIST,
            memberId2, ShareRequestMetadata.INITIAL_EPOCH, false, "id-2"));
    }

    @Test
    public void testNewContextExistingSessionNewRequestWithInitialEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        List<TopicIdPartition> reqData = List.of(tp0, tp1);

        // Create a new share session with an initial share fetch request
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());
        assertEquals(1, cache.size());

        // Sending another request with INITIAL_EPOCH and same share session key. This should return a new ShareSessionContext
        // and delete the older one.
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);
        assertFalse(((ShareSessionContext) context1).isSubsequent());
        assertEquals(1, cache.size());
    }

    @Test
    public void testNewContext() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData2 = List.of(tp0, tp1);

        String memberId = Uuid.randomUuid().toString();
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        ((ShareSessionContext) context2).shareFetchData().forEach(topicIdPartition -> assertTrue(reqData2.contains(topicIdPartition)));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId, respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId, memberId);

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
            shareSessionKey2.memberId(), 5, true, "id-2"));

        // Test trying to create a new session with a non-existent session key
        String memberId4 = Uuid.randomUuid().toString();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
            memberId4, 1, true, "id-3"));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
            shareSessionKey2.memberId(), 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context5);
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        ShareSessionContext shareSessionContext5 = (ShareSessionContext) context5;
        synchronized (shareSessionContext5.session()) {
            shareSessionContext5.session().partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                    TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                assertTrue(reqData2.contains(topicIdPartition));
            });
        }
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, memberId, respData2);
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(0, resp5.responseData(topicNames).size());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
            shareSessionKey2.memberId(), 5, true, CONNECTION_ID));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
            shareSessionKey2.memberId(), 2, true, CONNECTION_ID);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
            memberId, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, memberId, respData8);
        assertEquals(Errors.NONE, resp8.error());

        // Close the session.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseResponse =
            sharePartitionManager.releaseSession(groupId, memberId);
        assertTrue(releaseResponse.isDone());
        assertFalse(releaseResponse.isCompletedExceptionally());
        assertEquals(0, cache.size());
    }

    @Test
    public void testAcknowledgeSessionUpdateThrowsOnInitialEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        assertThrows(InvalidShareSessionEpochException.class,
            () -> sharePartitionManager.acknowledgeSessionUpdate("grp",
                Uuid.randomUuid().toString(), ShareRequestMetadata.INITIAL_EPOCH));
    }

    @Test
    public void testAcknowledgeSessionUpdateThrowsWhenShareSessionNotFound() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        // The share session corresponding to this memberId has not been created yet. This should throw an exception.
        assertThrows(ShareSessionNotFoundException.class,
            () -> sharePartitionManager.acknowledgeSessionUpdate("grp",
                Uuid.randomUuid().toString(), ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)));
    }

    @Test
    public void testAcknowledgeSessionUpdateThrowsInvalidShareSessionEpochException() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, List.of(tp0, tp1), EMPTY_PART_LIST,
            memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // The expected epoch from the share session should be 1, but we are passing 2. This should throw an exception.
        assertThrows(InvalidShareSessionEpochException.class,
            () -> sharePartitionManager.acknowledgeSessionUpdate("grp",
                memberId, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH))));
    }

    @Test
    public void testAcknowledgeSessionUpdateSuccessOnSubsequentEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, List.of(tp0, tp1), EMPTY_PART_LIST,
            memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // The expected epoch from the share session should be 1, and we are passing the same. So, execution should be successful.
        assertDoesNotThrow(
            () -> sharePartitionManager.acknowledgeSessionUpdate("grp",
                memberId, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH)));
    }

    @Test
    public void testAcknowledgeSessionUpdateSuccessOnFinalEpoch() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        // Create a new share session with an initial share fetch request
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, List.of(tp0, tp1), EMPTY_PART_LIST,
            memberId, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        // The expected epoch from the share session should be 1, but we are passing the Final Epoch (-1). This should throw an exception.
        assertDoesNotThrow(
            () -> sharePartitionManager.acknowledgeSessionUpdate("grp",
                memberId, ShareRequestMetadata.FINAL_EPOCH));
    }

    @Test
    public void testSubsequentShareSession() {
        sharePartitionManager = SharePartitionManagerBuilder.builder().build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        // Create a new share session with foo-0 and foo-1
        List<TopicIdPartition> reqData1 = List.of(tp0, tp1);

        String groupId = "grp";
        String memberId1 = Uuid.randomUuid().toString();

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp0.partition()));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, memberId1, respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent fetch request that removes foo-0 and adds bar-0
        List<TopicIdPartition> reqData2 = List.of(tp2);
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(tp0);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, removed2,
                memberId1, 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);

        Set<TopicIdPartition> expectedTopicIdPartitions2 = new HashSet<>();
        expectedTopicIdPartitions2.add(tp1);
        expectedTopicIdPartitions2.add(tp2);
        Set<TopicIdPartition> actualTopicIdPartitions2 = new HashSet<>();
        ShareSessionContext shareSessionContext = (ShareSessionContext) context2;
        shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
            TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                    TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
            actualTopicIdPartitions2.add(topicIdPartition);
        });

        assertEquals(expectedTopicIdPartitions2, actualTopicIdPartitions2);
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));
        respData2.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp2.partition()));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId1, respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(1, resp2.data().responses().size());
        assertEquals(barId, resp2.data().responses().stream().findFirst().get().topicId());
        assertEquals(1, resp2.data().responses().stream().findFirst().get().partitions().size());
        assertEquals(0, resp2.data().responses().stream().findFirst().get().partitions().get(0).partitionIndex());
        assertEquals(1, resp2.responseData(topicNames).size());
    }

    @Test
    public void testZeroSizeShareSession() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new share session with foo-0 and foo-1
        List<TopicIdPartition> reqData1 = List.of(foo0, foo1);

        String groupId = "grp";
        String memberId1 = Uuid.randomUuid().toString();

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData1.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, memberId1, respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share request that removes foo-0 and foo-1
        // Verify that the previous share session was closed.
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(foo0);
        removed2.add(foo1);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, removed2,
                memberId1, 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId1, respData2);
        assertTrue(resp2.responseData(topicNames).isEmpty());
        assertEquals(1, cache.size());
    }

    @Test
    public void testToForgetPartitions() {
        String groupId = "grp";
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        String memberId1 = Uuid.randomUuid().toString();

        List<TopicIdPartition> reqData1 = List.of(foo, bar);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertPartitionsPresent((ShareSessionContext) context1, List.of(foo, bar));

        mockUpdateAndGenerateResponseData(context1, groupId, memberId1);

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, List.of(foo),
                memberId1, 1, true, CONNECTION_ID);

        // So foo is removed but not the others.
        assertPartitionsPresent((ShareSessionContext) context2, List.of(bar));

        mockUpdateAndGenerateResponseData(context2, groupId, memberId1);

        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, List.of(bar),
                memberId1, 2, true, CONNECTION_ID);
        assertPartitionsPresent((ShareSessionContext) context3, EMPTY_PART_LIST);
    }

    // This test simulates a share session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
    // -- as though the topic is deleted and recreated.
    @Test
    public void testShareSessionUpdateTopicIdsBrokerSide() {
        String groupId = "grp";
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 1));

        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");

        // Create a new share session with foo-0 and bar-1
        List<TopicIdPartition> reqData1 = List.of(foo, bar);

        String memberId1 = Uuid.randomUuid().toString();
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);

        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(bar, new ShareFetchResponseData.PartitionData().setPartitionIndex(bar.partition()));
        respData1.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, memberId1, respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share fetch request as though no topics changed.
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
            memberId1, 1, true, CONNECTION_ID);

        assertInstanceOf(ShareSessionContext.class, context2);
        assertTrue(((ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
        respData2.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.INCONSISTENT_TOPIC_ID.code()));
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId1, respData2);
        assertEquals(Errors.NONE, resp2.error());
        // We should have the inconsistent topic ID error on the partition
        assertEquals(Errors.INCONSISTENT_TOPIC_ID.code(), resp2.responseData(topicNames).get(foo).errorCode());
    }

    @Test
    public void testGetErroneousAndValidTopicIdPartitions() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tpNull1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(null, 0));
        TopicIdPartition tpNull2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(null, 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData2 = List.of(tp0, tp1, tpNull1);

        String memberId2 = Uuid.randomUuid().toString();
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId2, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);
        assertFalse(((ShareSessionContext) context2).isSubsequent());
        assertErroneousAndValidTopicIdPartitions(context2.getErroneousAndValidTopicIdPartitions(), List.of(tpNull1), List.of(tp0, tp1));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        respData2.put(tpNull1, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId2, respData2);
        assertEquals(Errors.NONE, resp2.error());

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId, memberId2);

        // Check for throttled response
        ShareFetchResponse resp2Throttle = context2.throttleResponse(100);
        assertEquals(Errors.NONE, resp2Throttle.error());
        assertEquals(100, resp2Throttle.throttleTimeMs());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 5, true, CONNECTION_ID));

        // Test trying to create a new session with a non-existent session key
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                Uuid.randomUuid().toString(), 1, true, CONNECTION_ID));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context5);
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        assertErroneousAndValidTopicIdPartitions(context5.getErroneousAndValidTopicIdPartitions(), List.of(tpNull1), List.of(tp0, tp1));

        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, memberId2, respData2);
        assertEquals(Errors.NONE, resp5.error());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 5, true, CONNECTION_ID));

        // Test generating a throttled response for a subsequent share session
        List<TopicIdPartition> reqData7 = List.of(tpNull2);
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 2, true, CONNECTION_ID);
        // Check for throttled response
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        assertErroneousAndValidTopicIdPartitions(context7.getErroneousAndValidTopicIdPartitions(), List.of(tpNull1, tpNull2), List.of(tp0, tp1));

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
                memberId2, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        assertErroneousAndValidTopicIdPartitions(context8.getErroneousAndValidTopicIdPartitions(), EMPTY_PART_LIST, EMPTY_PART_LIST);
        // Check for throttled response
        ShareFetchResponse resp8 = context8.throttleResponse(100);
        assertEquals(Errors.NONE, resp8.error());
        assertEquals(100, resp8.throttleTimeMs());

        // Close the session.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseResponse =
            sharePartitionManager.releaseSession(groupId, memberId2);
        assertTrue(releaseResponse.isDone());
        assertFalse(releaseResponse.isCompletedExceptionally());
        assertEquals(0, cache.size());
    }

    @Test
    public void testShareFetchContextResponseSize() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        List<TopicIdPartition> reqData2 = List.of(tp0, tp1);

        // For response size expected value calculation
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        short version = ApiKeys.SHARE_FETCH.latestVersion();

        String memberId2 = Uuid.randomUuid().toString();
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId2, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize2 = context2.responseSize(respData2, version);
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId2, respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp2.data().size(objectSerializationCache, version), respSize2);

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId,
            memberId2);

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 5, true, CONNECTION_ID));

        // Test trying to create a new session with a non-existent session key
        String memberId4 = Uuid.randomUuid().toString();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                memberId4, 1, true, CONNECTION_ID));

        // Continue the first share session we created.
        List<TopicIdPartition> reqData5 = List.of(tp2);
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context5);
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        respData5.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        int respSize5 = context5.responseSize(respData5, version);
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, memberId2, respData5);
        assertEquals(Errors.NONE, resp5.error());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp5.data().size(objectSerializationCache, version), respSize5);

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 5, true, CONNECTION_ID));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
                shareSessionKey2.memberId(), 2, true, CONNECTION_ID);

        int respSize7 = context7.responseSize(respData2, version);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + new ShareFetchResponseData().size(objectSerializationCache, version), respSize7);

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
                memberId2, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize8 = context8.responseSize(respData8, version);
        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, memberId2, respData8);
        assertEquals(Errors.NONE, resp8.error());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp8.data().size(objectSerializationCache, version), respSize8);
    }

    @Test
    public void testCachedTopicPartitionsWithNoTopicPartitions() {
        ShareSessionCache cache = new ShareSessionCache(10);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        List<TopicIdPartition> result = sharePartitionManager.cachedTopicIdPartitionsInShareSession("grp", Uuid.randomUuid().toString());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testCachedTopicPartitionsForValidShareSessions() {
        ShareSessionCache cache = new ShareSessionCache(10);

        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));
        String groupId = "grp";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        when(sp0.releaseAcquiredRecords(ArgumentMatchers.eq(String.valueOf(memberId1)))).thenReturn(CompletableFuture.completedFuture(null));
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(String.valueOf(memberId1)))).thenReturn(CompletableFuture.completedFuture(null));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(String.valueOf(memberId1)))).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache)
                .withPartitionCache(partitionCache)
                .build();


        // Create a new share session with an initial share fetch request.
        List<TopicIdPartition> reqData1 = List.of(tp0, tp1);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, memberId1, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context1);
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareSessionKey shareSessionKey1 = new ShareSessionKey(groupId,
                memberId1);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, memberId1, respData1);
        assertEquals(Errors.NONE, resp1.error());

        assertEquals(Set.of(tp0, tp1),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Create a new share session with an initial share fetch request.
        List<TopicIdPartition> reqData2 = List.of(tp2);

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, memberId2, ShareRequestMetadata.INITIAL_EPOCH, false, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context2);
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId, memberId2);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, memberId2, respData2);
        assertEquals(Errors.NONE, resp2.error());

        assertEquals(List.of(tp2), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Continue the first share session we created.
        List<TopicIdPartition> reqData3 = List.of(tp2);
        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, reqData3, EMPTY_PART_LIST,
                shareSessionKey1.memberId(), 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context3);
        assertTrue(((ShareSessionContext) context3).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        ShareFetchResponse resp3 = context3.updateAndGenerateResponseData(groupId, memberId1, respData3);
        assertEquals(Errors.NONE, resp3.error());

        assertEquals(Set.of(tp0, tp1, tp2),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Continue the second session we created.
        List<TopicIdPartition> reqData4 = List.of(tp3);
        ShareFetchContext context4 = sharePartitionManager.newContext(groupId, reqData4, List.of(tp2),
                shareSessionKey2.memberId(), 1, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context4);
        assertTrue(((ShareSessionContext) context4).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData4 = new LinkedHashMap<>();
        respData4.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        ShareFetchResponse resp4 = context4.updateAndGenerateResponseData(groupId, memberId2, respData4);
        assertEquals(Errors.NONE, resp4.error());

        assertEquals(List.of(tp3), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Get the final share session.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, EMPTY_PART_LIST,
                memberId1, ShareRequestMetadata.FINAL_EPOCH, true, CONNECTION_ID);
        assertEquals(FinalContext.class, context5.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, memberId1, respData5);
        assertEquals(Errors.NONE, resp5.error());

        assertFalse(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1).isEmpty());

        // Close the first session.
        sharePartitionManager.releaseSession(groupId, memberId1);
        assertTrue(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1).isEmpty());

        // Continue the second share session .
        ShareFetchContext context6 = sharePartitionManager.newContext(groupId, EMPTY_PART_LIST, List.of(tp3),
                shareSessionKey2.memberId(), 2, true, CONNECTION_ID);
        assertInstanceOf(ShareSessionContext.class, context6);
        assertTrue(((ShareSessionContext) context6).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData6 = new LinkedHashMap<>();
        ShareFetchResponse resp6 = context6.updateAndGenerateResponseData(groupId, memberId2, respData6);
        assertEquals(Errors.NONE, resp6.error());

        assertEquals(EMPTY_PART_LIST, sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));
    }

    @Test
    public void testSharePartitionKey() {
        SharePartitionKey sharePartitionKey1 = new SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionKey sharePartitionKey2 = new SharePartitionKey("mock-group-2",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionKey sharePartitionKey3 = new SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(1L, 1L), new TopicPartition("test-1", 0)));
        SharePartitionKey sharePartitionKey4 = new SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 1)));
        SharePartitionKey sharePartitionKey5 = new SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 0L), new TopicPartition("test-2", 0)));
        SharePartitionKey sharePartitionKey1Copy = new SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));

        assertEquals(sharePartitionKey1, sharePartitionKey1Copy);
        assertNotEquals(sharePartitionKey1, sharePartitionKey2);
        assertNotEquals(sharePartitionKey1, sharePartitionKey3);
        assertNotEquals(sharePartitionKey1, sharePartitionKey4);
        assertNotEquals(sharePartitionKey1, sharePartitionKey5);
        assertNotNull(sharePartitionKey1);
    }

    @Test
    public void testMultipleSequentialShareFetches() {
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(barId, new TopicPartition("bar", 1));
        TopicIdPartition tp4 = new TopicIdPartition(fooId, new TopicPartition("foo", 2));
        TopicIdPartition tp5 = new TopicIdPartition(barId, new TopicPartition("bar", 2));
        TopicIdPartition tp6 = new TopicIdPartition(fooId, new TopicPartition("foo", 3));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1, tp2, tp3, tp4, tp5, tp6);

        mockFetchOffsetForTimestamp(mockReplicaManager);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp0, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp2, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp3, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp4, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp5, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp6, 1);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        doAnswer(invocation -> buildLogReadResult(topicIdPartitions)).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = sharePartitionManager.fetchMessages(
            groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 1, MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        assertTrue(future.isDone());
        Mockito.verify(mockReplicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 3,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        assertTrue(future.isDone());
        Mockito.verify(mockReplicaManager, times(2)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 10,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        assertTrue(future.isDone());
        Mockito.verify(mockReplicaManager, times(3)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Should have 6 total fetches, 3 fetches for topic foo (though 4 partitions but 3 fetches) and 3
        // fetches for topic bar (though 3 partitions but 3 fetches).
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(6, 0, 0, 0),
            Map.of("foo", new TopicMetrics(3, 0, 0, 0), "bar", new TopicMetrics(3, 0, 0, 0))
        );
    }

    @Test
    public void testReplicaManagerFetchShouldNotProceed() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        Mockito.verify(mockReplicaManager, times(0)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = future.join();
        assertEquals(0, result.size());
        // Should have 1 fetch recorded and no failed as the fetch did complete without error.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(1, 0, 0, 0),
            Map.of("foo", new TopicMetrics(1, 0, 0, 0))
        );
    }

    @Test
    public void testReplicaManagerFetchShouldProceed() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        mockFetchOffsetForTimestamp(mockReplicaManager);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp0, 1);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        doAnswer(invocation -> buildLogReadResult(topicIdPartitions)).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        // Since the nextFetchOffset does not point to endOffset + 1, i.e. some of the records in the cachedState are AVAILABLE,
        // even though the maxInFlightMessages limit is exceeded, replicaManager.readFromLog should be called
        Mockito.verify(mockReplicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        // Should have 1 fetch recorded.
        assertEquals(1, brokerTopicStats.allTopicsStats().totalShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.numTopics());
        assertEquals(1, brokerTopicStats.topicStats(tp0.topic()).totalShareFetchRequestRate().count());
    }

    @Test
    public void testCloseSharePartitionManager() throws Exception {
        Timer timer = Mockito.mock(SystemTimerReaper.class);
        ShareGroupMetrics shareGroupMetrics = Mockito.mock(ShareGroupMetrics.class);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withTimer(timer)
            .withShareGroupMetrics(shareGroupMetrics)
            .build();

        // Verify that 0 calls are made to timer.close() and shareGroupMetrics.close().
        Mockito.verify(timer, times(0)).close();
        Mockito.verify(shareGroupMetrics, times(0)).close();
        // Closing the sharePartitionManager closes timer object in sharePartitionManager.
        sharePartitionManager.close();
        // Verify that the timer object in sharePartitionManager is closed by checking the calls to timer.close() and shareGroupMetrics.close().
        Mockito.verify(timer, times(1)).close();
        Mockito.verify(shareGroupMetrics, times(1)).close();
    }

    @Test
    public void testReleaseSessionSuccess() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("baz", 4));

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(null));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(FutureUtils.failedFuture(
                new InvalidRecordStateException("Unable to release acquired records for the batch")
        ));

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);

        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap = new ImplicitLinkedHashCollection<>(3);
        partitionMap.add(new CachedSharePartition(tp1));
        partitionMap.add(new CachedSharePartition(tp2));
        partitionMap.add(new CachedSharePartition(tp3));
        when(shareSession.partitionMap()).thenReturn(partitionMap);

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.INVALID_RECORD_STATE.code(), result.get(tp2).errorCode());
        assertEquals("Unable to release acquired records for the batch", result.get(tp2).errorMessage());
        // tp3 was not a part of partitionCache.
        assertEquals(4, result.get(tp3).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp3).errorCode());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.message(), result.get(tp3).errorMessage());
        // Shouldn't have any metrics for fetch and acknowledge.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 0, 0),
            Map.of()
        );
    }

    @Test
    public void testReleaseSessionWithIncorrectGroupId() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);

        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap = new ImplicitLinkedHashCollection<>(3);
        partitionMap.add(new CachedSharePartition(tp1));
        when(shareSession.partitionMap()).thenReturn(partitionMap);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        // Calling releaseSession with incorrect groupId.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession("grp-2", memberId);
        assertTrue(resultFuture.isDone());
        assertTrue(resultFuture.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, resultFuture::get);
        assertInstanceOf(ShareSessionNotFoundException.class, exception.getCause());
    }

    @Test
    public void testReleaseSessionWithIncorrectMemberId() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        // Member with random Uuid so that it does not match the memberId.
        when(cache.get(new ShareSessionKey(groupId, Uuid.randomUuid().toString()))).thenReturn(shareSession);

        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap = new ImplicitLinkedHashCollection<>(3);
        partitionMap.add(new CachedSharePartition(tp1));
        when(shareSession.partitionMap()).thenReturn(partitionMap);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId);
        assertTrue(resultFuture.isDone());
        assertTrue(resultFuture.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, resultFuture::get);
        assertInstanceOf(ShareSessionNotFoundException.class, exception.getCause());
    }

    @Test
    public void testReleaseSessionWithEmptyTopicPartitions() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);
        when(shareSession.partitionMap()).thenReturn(new ImplicitLinkedHashCollection<>());

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        // Empty list of TopicIdPartitions to releaseSession. This should return an empty map.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testReleaseSessionWithNullShareSession() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        ShareSessionCache cache = mock(ShareSessionCache.class);
        // Null share session in get response so empty topic partitions should be returned.
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(null);
        // Make the response not null for remove so can further check for the return value from topic partitions.
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(mock(ShareSession.class));

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
            sharePartitionManager.releaseSession(groupId, memberId);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testAcknowledgeSinglePartition() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp = mock(SharePartition.class);

        when(sp.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp), sp);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, List.of(
            new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp).errorCode());

        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 1, 0),
            Map.of("foo", new TopicMetrics(0, 0, 1, 0))
        );
    }

    @Test
    public void testAcknowledgeMultiplePartition() throws Exception {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        when(sp1.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(sp2.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(sp3.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCache.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withShareGroupMetrics(shareGroupMetrics)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp1, List.of(
                new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));
        acknowledgeTopics.put(tp2, List.of(
                new ShareAcknowledgementBatch(15, 26, List.of((byte) 2)),
                new ShareAcknowledgementBatch(34, 56, List.of((byte) 2))
        ));
        acknowledgeTopics.put(tp3, List.of(
                new ShareAcknowledgementBatch(4, 15, List.of((byte) 3)),
                new ShareAcknowledgementBatch(16, 21, List.of((byte) 3))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(0, result.get(tp2).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp2).errorCode());
        assertEquals(0, result.get(tp3).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp3).errorCode());

        assertEquals(42, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.ACCEPT.id).count());
        assertEquals(35, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.RELEASE.id).count());
        assertEquals(18, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.REJECT.id).count());
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.ACCEPT.id).meanRate() > 0);
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.RELEASE.id).meanRate() > 0);
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.REJECT.id).meanRate() > 0);

        // Should have 3 successful acknowledgement and 1 successful acknowledgement per topic.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 3, 0),
            Map.of(tp1.topic(), new TopicMetrics(0, 0, 1, 0), tp2.topic(), new TopicMetrics(0, 0, 1, 0), tp3.topic(), new TopicMetrics(0, 0, 1, 0))
        );
        shareGroupMetrics.close();
    }

    @Test
    public void testAcknowledgeIndividualOffsets() throws Exception {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        List<ShareAcknowledgementBatch> ack1 = List.of(
            new ShareAcknowledgementBatch(12, 12, List.of((byte) 1)));
        List<ShareAcknowledgementBatch> ack2 = List.of(
            new ShareAcknowledgementBatch(15, 20, List.of((byte) 2, (byte) 3, (byte) 2, (byte) 2, (byte) 3, (byte) 2)));
        when(sp1.acknowledge(memberId, ack1)).thenReturn(CompletableFuture.completedFuture(null));
        when(sp2.acknowledge(memberId, ack2)).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);

        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withShareGroupMetrics(shareGroupMetrics)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = Map.of(tp1, ack1, tp2, ack2);
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
            sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(2, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(0, result.get(tp2).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp2).errorCode());

        assertEquals(1, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.ACCEPT.id).count());
        assertEquals(4, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.RELEASE.id).count());
        assertEquals(2, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.REJECT.id).count());
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.ACCEPT.id).meanRate() > 0);
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.RELEASE.id).meanRate() > 0);
        assertTrue(shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.REJECT.id).meanRate() > 0);

        shareGroupMetrics.close();
    }

    @Test
    public void testAcknowledgeIncorrectGroupId() {
        String groupId = "grp";
        String groupId2 = "grp2";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp = mock(SharePartition.class);

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp), sp);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .withShareGroupMetrics(shareGroupMetrics)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, List.of(
            new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId2, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp).errorCode());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.message(), result.get(tp).errorMessage());
        // No metric should be recorded as acknowledge failed.
        assertEquals(0, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.ACCEPT.id).count());
        assertEquals(0, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.RELEASE.id).count());
        assertEquals(0, shareGroupMetrics.recordAcknowledgementMeter(AcknowledgeType.REJECT.id).count());
        // Should have 1 acknowledge recorded and 1 failed.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 1, 1),
            Map.of(tp.topic(), new TopicMetrics(0, 0, 1, 1))
        );
    }

    @Test
    public void testAcknowledgeIncorrectMemberId() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp = mock(SharePartition.class);
        when(sp.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(FutureUtils.failedFuture(
                new InvalidRequestException("Member is not the owner of batch record")
        ));
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp), sp);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, List.of(
            new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
            sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.INVALID_REQUEST.code(), result.get(tp).errorCode());
        assertEquals("Member is not the owner of batch record", result.get(tp).errorMessage());
        // Should have 1 acknowledge recorded and 1 failed.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 1, 1),
            Map.of(tp.topic(), new TopicMetrics(0, 0, 1, 1))
        );
    }

    @Test
    public void testAcknowledgeEmptyPartitionCacheMap() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo4", 3));
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, List.of(
                new ShareAcknowledgementBatch(78, 90, List.of((byte) 2)),
                new ShareAcknowledgementBatch(94, 99, List.of((byte) 2))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(3, result.get(tp).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp).errorCode());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.message(), result.get(tp).errorMessage());
        // Should have 1 acknowledge recorded and 1 failed.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(0, 0, 1, 1),
            Map.of(tp.topic(), new TopicMetrics(0, 0, 1, 1))
        );
    }

    @Test
    public void testAcknowledgeCompletesDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));

        List<TopicIdPartition> topicIdPartitions = List.of(tp1, tp2);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        // mocked share partitions sp1 and sp2 can be acquired once there is an acknowledgement for it.
        doAnswer(invocation -> {
            when(sp1.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp1).acknowledge(ArgumentMatchers.eq(memberId), any());
        doAnswer(invocation -> {
            when(sp2.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp2).acknowledge(ArgumentMatchers.eq(memberId), any());

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);

        ShareFetch shareFetch = new ShareFetch(
            FETCH_PARAMS,
            groupId,
            Uuid.randomUuid().toString(),
            new CompletableFuture<>(),
            topicIdPartitions,
            BATCH_OPTIMIZED,
            BATCH_SIZE,
            100,
            brokerTopicStats);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 2);

        // Initially you cannot acquire records for both sp1 and sp2.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp1.acquire(anyString(), any(ShareAcquireMode.class), anyInt(), anyInt(), anyLong(), any(), any())).thenReturn(ShareAcquiredRecords.empty());
        when(sp2.acquire(anyString(), any(ShareAcquireMode.class), anyInt(), anyInt(), anyLong(), any(), any())).thenReturn(ShareAcquiredRecords.empty());

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(mockReplicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        doAnswer(invocation -> buildLogReadResult(List.of(tp1))).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp1, List.of(
                new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));

        assertEquals(2, delayedShareFetchPurgatory.watched());
        // Acknowledgement request for sp1.
        sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);

        // Since sp1 is acknowledged, the delayedShareFetchPurgatory should have 1 watched key corresponding to sp2.
        assertEquals(1, delayedShareFetchPurgatory.watched());

        Mockito.verify(sp1, times(1)).nextFetchOffset();
        Mockito.verify(sp2, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        // Should have 1 acknowledge recorded as other topic is acknowledgement request is not sent.
        assertEquals(1, brokerTopicStats.allTopicsStats().totalShareAcknowledgementRequestRate().count());
        assertEquals(1, brokerTopicStats.numTopics());
        assertEquals(1, brokerTopicStats.topicStats(tp1.topic()).totalShareAcknowledgementRequestRate().count());
    }

    @Test
    public void testAcknowledgeDoesNotCompleteDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        List<TopicIdPartition> topicIdPartitions = List.of(tp1, tp2);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        // mocked share partitions sp1, sp2 and sp3 can be acquired once there is an acknowledgement for it.
        doAnswer(invocation -> {
            when(sp1.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp1).acknowledge(ArgumentMatchers.eq(memberId), any());
        doAnswer(invocation -> {
            when(sp2.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp2).acknowledge(ArgumentMatchers.eq(memberId), any());
        doAnswer(invocation -> {
            when(sp3.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp3).acknowledge(ArgumentMatchers.eq(memberId), any());

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCache.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareFetch shareFetch = new ShareFetch(
            FETCH_PARAMS,
            groupId,
            Uuid.randomUuid().toString(),
            new CompletableFuture<>(),
            topicIdPartitions,
            BATCH_OPTIMIZED,
            BATCH_SIZE,
            100,
            brokerTopicStats);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Initially you cannot acquire records for both all 3 share partitions.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp3.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp3.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);
        sharePartitions.put(tp3, sp3);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(mockReplicaManager)
            .withSharePartitions(sharePartitions)
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp3, List.of(
                new ShareAcknowledgementBatch(12, 20, List.of((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, List.of((byte) 1))
        ));

        // Acknowledgement request for sp3.
        sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);

        // Since neither sp1 and sp2 have been acknowledged, the delayedShareFetchPurgatory should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        Mockito.verify(sp1, times(0)).nextFetchOffset();
        Mockito.verify(sp2, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        // Should have 1 acknowledge recorded as other 2 topics acknowledgement request is not sent.
        assertEquals(1, brokerTopicStats.allTopicsStats().totalShareAcknowledgementRequestRate().count());
        assertEquals(1, brokerTopicStats.numTopics());
        assertEquals(1, brokerTopicStats.topicStats(tp3.topic()).totalShareAcknowledgementRequestRate().count());
    }

    @Test
    public void testReleaseSessionCompletesDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        List<TopicIdPartition> topicIdPartitions = List.of(tp1, tp2);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);

        // mocked share partitions sp1 and sp2 can be acquired once there is a release acquired records on session close request for it.
        doAnswer(invocation -> {
            when(sp1.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp1).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));
        doAnswer(invocation -> {
            when(sp2.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp2).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));
        when(sp3.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCache.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareFetch shareFetch = new ShareFetch(
            FETCH_PARAMS,
            groupId,
            Uuid.randomUuid().toString(),
            new CompletableFuture<>(),
            topicIdPartitions,
            BATCH_OPTIMIZED,
            BATCH_SIZE,
            100,
            brokerTopicStats);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 1);

        // Initially you cannot acquire records for both sp1 and sp2.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        sharePartitionManager = spy(SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withCache(cache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(mockReplicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        // The share session for this share group member returns tp1 and tp3, tp1 is common in both the delayed fetch request and the share session.
        when(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId)).thenReturn(List.of(tp1, tp3));

        doAnswer(invocation -> buildLogReadResult(List.of(tp1))).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        when(sp1.acquire(anyString(), any(), anyInt(), anyInt(), anyLong(), any(), any())).thenReturn(new ShareAcquiredRecords(EMPTY_ACQUIRED_RECORDS, 0));
        // Release acquired records on session close request for tp1 and tp3.
        sharePartitionManager.releaseSession(groupId, memberId);

        // Since sp1's request to release acquired records on session close is completed, the delayedShareFetchPurgatory
        // should have 1 watched key corresponding to sp2.
        assertEquals(1, delayedShareFetchPurgatory.watched());

        Mockito.verify(sp1, times(1)).nextFetchOffset();
        Mockito.verify(sp2, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testReleaseSessionDoesNotCompleteDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        List<TopicIdPartition> topicIdPartitions = List.of(tp1, tp2);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);

        // mocked share partitions sp1, sp2 and sp3 can be acquired once there is a release acquired records on session close for it.
        doAnswer(invocation -> {
            when(sp1.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp1).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));
        doAnswer(invocation -> {
            when(sp2.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp2).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));
        doAnswer(invocation -> {
            when(sp3.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp3).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCache.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareFetch shareFetch = new ShareFetch(
            FETCH_PARAMS,
            groupId,
            Uuid.randomUuid().toString(),
            new CompletableFuture<>(),
            topicIdPartitions,
            BATCH_OPTIMIZED,
            BATCH_SIZE,
            100,
            brokerTopicStats);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Initially you cannot acquire records for both all 3 share partitions.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp3.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp3.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        sharePartitionManager = spy(SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withCache(cache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);
        sharePartitions.put(tp3, sp3);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(mockReplicaManager)
            .withSharePartitions(sharePartitions)
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        // The share session for this share group member returns tp1 and tp3. No topic partition is common in
        // both the delayed fetch request and the share session.
        when(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId)).thenReturn(List.of(tp3));

        // Release acquired records on session close for sp3.
        sharePartitionManager.releaseSession(groupId, memberId);

        // Since neither sp1 and sp2 are a part of the release acquired records request on session close, the
        // delayedShareFetchPurgatory should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        Mockito.verify(sp1, times(0)).nextFetchOffset();
        Mockito.verify(sp2, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testPendingInitializationShouldCompleteFetchRequest() throws Exception {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);

        // Keep the initialization future pending, so fetch request is stuck.
        CompletableFuture<Void> pendingInitializationFuture = new CompletableFuture<>();
        when(sp0.maybeInitialize()).thenReturn(pendingInitializationFuture);
        when(sp0.loadStartTimeMs()).thenReturn(10L);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(100L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTime(time)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        // Verify that the fetch request is completed.
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        assertTrue(future.join().isEmpty());
        // Verify that replica manager fetch is not called.
        Mockito.verify(mockReplicaManager, times(0)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertFalse(pendingInitializationFuture.isDone());
        assertEquals(0, shareGroupMetrics.partitionLoadTimeMs().count());
        // Complete the pending initialization future.
        pendingInitializationFuture.complete(null);
        // Verify the partition load time metrics.
        assertEquals(1, shareGroupMetrics.partitionLoadTimeMs().count());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().min());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().max());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().sum());
        // Should have 1 fetch recorded.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(1, 0, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(1, 0, 0, 0))
        );
        shareGroupMetrics.close();
    }

    @Test
    public void testPartitionLoadTimeMetricWithMultiplePartitions() throws Exception {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);

        // Keep the initialization future pending, so fetch request is stuck.
        CompletableFuture<Void> pendingInitializationFuture1 = new CompletableFuture<>();
        when(sp0.maybeInitialize()).thenReturn(pendingInitializationFuture1);
        when(sp0.loadStartTimeMs()).thenReturn(10L);

        CompletableFuture<Void> pendingInitializationFuture2 = new CompletableFuture<>();
        when(sp1.maybeInitialize()).thenReturn(pendingInitializationFuture2);
        when(sp1.loadStartTimeMs()).thenReturn(40L);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(100L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTime(time)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        // Verify that the fetch request is completed.
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        assertFalse(pendingInitializationFuture1.isDone());
        assertFalse(pendingInitializationFuture2.isDone());
        assertEquals(0, shareGroupMetrics.partitionLoadTimeMs().count());
        // Complete the first pending initialization future.
        pendingInitializationFuture1.complete(null);
        // Verify the partition load time metrics for first partition.
        assertEquals(1, shareGroupMetrics.partitionLoadTimeMs().count());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().min());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().max());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().sum());
        // Complete the second pending initialization future.
        pendingInitializationFuture2.complete(null);
        // Verify the partition load time metrics for both partitions.
        assertEquals(2, shareGroupMetrics.partitionLoadTimeMs().count());
        assertEquals(60.0, shareGroupMetrics.partitionLoadTimeMs().min());
        assertEquals(90.0, shareGroupMetrics.partitionLoadTimeMs().max());
        assertEquals(150.0, shareGroupMetrics.partitionLoadTimeMs().sum());
        shareGroupMetrics.close();
    }

    @Test
    public void testDelayedInitializationShouldCompleteFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);

        // Keep the 2 initialization futures pending and 1 completed with leader not available exception.
        CompletableFuture<Void> pendingInitializationFuture1 = new CompletableFuture<>();
        CompletableFuture<Void> pendingInitializationFuture2 = new CompletableFuture<>();
        when(sp0.maybeInitialize()).
            thenReturn(pendingInitializationFuture1)
            .thenReturn(pendingInitializationFuture2)
            .thenReturn(CompletableFuture.failedFuture(new LeaderNotAvailableException("Leader not available")));

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> shareFetchPurgatorySpy = spy(new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true));
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, shareFetchPurgatorySpy);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        // Send 3 requests for share fetch for same share partition.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future1 =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future2 =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future3 =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);

        Mockito.verify(sp0, times(3)).maybeInitialize();
        Mockito.verify(mockReplicaManager, times(3)).addDelayedShareFetchRequest(any(), any());
        Mockito.verify(shareFetchPurgatorySpy, times(3)).tryCompleteElseWatch(any(), any());
        Mockito.verify(shareFetchPurgatorySpy, times(0)).checkAndComplete(any());

        // All 3 requests should be pending.
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());

        // Complete one pending initialization future.
        pendingInitializationFuture1.complete(null);
        Mockito.verify(mockReplicaManager, times(1)).completeDelayedShareFetchRequest(any());
        Mockito.verify(shareFetchPurgatorySpy, times(1)).checkAndComplete(any());

        pendingInitializationFuture2.complete(null);
        Mockito.verify(mockReplicaManager, times(2)).completeDelayedShareFetchRequest(any());
        Mockito.verify(shareFetchPurgatorySpy, times(2)).checkAndComplete(any());

        // Verify that replica manager fetch is not called.
        Mockito.verify(mockReplicaManager, times(0)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        // Should have 3 fetch recorded.
        assertEquals(3, brokerTopicStats.allTopicsStats().totalShareFetchRequestRate().count());
        assertEquals(1, brokerTopicStats.numTopics());
        assertEquals(3, brokerTopicStats.topicStats(tp0.topic()).totalShareFetchRequestRate().count());
    }

    @Test
    public void testSharePartitionInitializationExceptions() throws Exception {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        // Return LeaderNotAvailableException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new LeaderNotAvailableException("Leader not available")));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        // Exception for client should not occur for LeaderNotAvailableException, this exception is to communicate
        // between SharePartitionManager and SharePartition to retry the request as SharePartition is not yet ready.
        assertFalse(future.isCompletedExceptionally());
        assertTrue(future.join().isEmpty());
        Mockito.verify(sp0, times(0)).markFenced();
        // Verify that the share partition is still in the cache on LeaderNotAvailableException.
        assertEquals(1, partitionCache.size());

        // Return IllegalStateException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new IllegalStateException("Illegal state")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Illegal state");
        Mockito.verify(sp0, times(1)).markFenced();
        assertTrue(partitionCache.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return CoordinatorNotAvailableException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new CoordinatorNotAvailableException("Coordinator not available")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.COORDINATOR_NOT_AVAILABLE, "Coordinator not available");
        Mockito.verify(sp0, times(2)).markFenced();
        assertTrue(partitionCache.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return InvalidRequestException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new InvalidRequestException("Invalid request")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.INVALID_REQUEST, "Invalid request");
        Mockito.verify(sp0, times(3)).markFenced();
        assertTrue(partitionCache.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return FencedStateEpochException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new FencedStateEpochException("Fenced state epoch")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.FENCED_STATE_EPOCH, "Fenced state epoch");
        Mockito.verify(sp0, times(4)).markFenced();
        assertTrue(partitionCache.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return NotLeaderOrFollowerException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new NotLeaderOrFollowerException("Not leader or follower")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER, "Not leader or follower");
        Mockito.verify(sp0, times(5)).markFenced();
        assertTrue(partitionCache.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return RuntimeException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new RuntimeException("Runtime exception")));
        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Runtime exception");
        Mockito.verify(sp0, times(6)).markFenced();
        assertTrue(partitionCache.isEmpty());
        // Should have 7 fetch recorded and 6 failures as 1 fetch was waiting on initialization and
        // didn't error out.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(7, 6, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(7, 6, 0, 0))
        );
    }

    @Test
    public void testShareFetchProcessingExceptions() throws Exception {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartitionCache partitionCache = mock(SharePartitionCache.class);
        // Throw the exception for first fetch request. Return share partition for next.
        when(partitionCache.computeIfAbsent(any(), any()))
            .thenThrow(new RuntimeException("Error creating instance"));

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Error creating instance");
        // Should have 1 fetch recorded and 1 failure.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(1, 1, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(1, 1, 0, 0))
        );
    }

    @Test
    public void testSharePartitionInitializationFailure() throws Exception {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        // Send map to check no share partition is created.
        SharePartitionCache partitionCache = new SharePartitionCache();
        // Validate when partition is not the leader.
        Partition partition = mock(Partition.class);
        when(partition.isLeader()).thenReturn(false);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        // First check should throw KafkaStorageException, second check should return partition which
        // is not leader.
        when(replicaManager.getPartitionOrException(any(TopicPartition.class)))
            .thenThrow(new KafkaStorageException("Exception"))
            .thenReturn(partition);
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        // Validate when exception is thrown.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.KAFKA_STORAGE_ERROR, "Exception");
        assertTrue(partitionCache.isEmpty());

        // Validate when partition is not leader.
        future = sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER);
        assertTrue(partitionCache.isEmpty());
        // Should have 2 fetch recorded and 2 failure.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(2, 2, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(2, 2, 0, 0))
        );
    }

    @Test
    public void testSharePartitionPartialInitializationFailure() throws Exception {
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        // For tp0, share partition instantiation will fail.
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        // For tp1, share fetch should succeed.
        TopicIdPartition tp1 = new TopicIdPartition(memberId1, new TopicPartition("foo", 1));
        // For tp2, share partition initialization will fail.
        TopicIdPartition tp2 = new TopicIdPartition(memberId1, new TopicPartition("foo", 2));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1, tp2);

        // Mark partition0 as not the leader.
        Partition partition0 = mock(Partition.class);
        when(partition0.isLeader()).thenReturn(false);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getPartitionOrException(any(TopicPartition.class)))
            .thenReturn(partition0);

        // Mock share partition for tp1, so it can succeed.
        SharePartition sp1 = mock(SharePartition.class);
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);

        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp1.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        when(sp1.acquire(anyString(), any(ShareAcquireMode.class), anyInt(), anyInt(), anyLong(), any(), any())).thenReturn(new ShareAcquiredRecords(EMPTY_ACQUIRED_RECORDS, 0));

        // Fail initialization for tp2.
        SharePartition sp2 = mock(SharePartition.class);
        partitionCache.put(new SharePartitionKey(groupId, tp2), sp2);
        when(sp2.maybeInitialize()).thenReturn(CompletableFuture.failedFuture(new FencedStateEpochException("Fenced state epoch")));

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(replicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp1, 1);

        doAnswer(invocation -> buildLogReadResult(List.of(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withPartitionCache(partitionCache)
            .withBrokerTopicStats(brokerTopicStats)
            .withTimer(mockTimer)
            .build();

        // Validate when exception is thrown.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());

        Map<TopicIdPartition, PartitionData> partitionDataMap = future.get();
        assertEquals(3, partitionDataMap.size());
        assertTrue(partitionDataMap.containsKey(tp0));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(), partitionDataMap.get(tp0).errorCode());
        assertTrue(partitionDataMap.containsKey(tp1));
        assertEquals(Errors.NONE.code(), partitionDataMap.get(tp1).errorCode());
        assertTrue(partitionDataMap.containsKey(tp2));
        assertEquals(Errors.FENCED_STATE_EPOCH.code(), partitionDataMap.get(tp2).errorCode());
        assertEquals("Fenced state epoch", partitionDataMap.get(tp2).errorMessage());

        Mockito.verify(replicaManager, times(1)).completeDelayedShareFetchRequest(
            new DelayedShareFetchGroupKey(groupId, tp2));
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        // Should have 1 fetch recorded and 1 failure as single topic has multiple partition fetch
        // and failure.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(1, 1, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(1, 1, 0, 0))
        );
    }

    @Test
    public void testReplicaManagerFetchException() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        doThrow(new RuntimeException("Exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Exception");
        // Verify that the share partition is still in the cache on exception.
        assertEquals(1, partitionCache.size());

        // Throw NotLeaderOrFollowerException from replica manager fetch which should evict instance from the cache.
        doThrow(new NotLeaderOrFollowerException("Leader exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER, "Leader exception");
        assertTrue(partitionCache.isEmpty());
        // Should have 2 fetch recorded and 2 failures.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(2, 2, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(2, 2, 0, 0))
        );
    }

    @Test
    public void testReplicaManagerFetchMultipleSharePartitionsException() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));

        SharePartition sp1 = mock(SharePartition.class);
        // Do not make the share partition acquirable hence it shouldn't be removed from the cache,
        // as it won't be part of replica manager readFromLog request.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(false);
        when(sp1.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        partitionCache.put(new SharePartitionKey(groupId, tp1), sp1);

        Timer mockTimer = systemTimerReaper();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Throw FencedStateEpochException from replica manager fetch which should evict instance from the cache.
        doThrow(new FencedStateEpochException("Fenced exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
                MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        validateShareFetchFutureException(future, tp0, Errors.FENCED_STATE_EPOCH, "Fenced exception");
        // Verify that tp1 is still in the cache on exception.
        assertEquals(1, partitionCache.size());
        assertEquals(sp1, partitionCache.get(new SharePartitionKey(groupId, tp1)));

        // Make sp1 acquirable and add sp0 back in partition cache. Both share partitions should be
        // removed from the cache.
        when(sp1.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        partitionCache.put(new SharePartitionKey(groupId, tp0), sp0);
        // Throw FencedStateEpochException from replica manager fetch which should evict instance from the cache.
        doThrow(new FencedStateEpochException("Fenced exception again")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        validateShareFetchFutureException(future, List.of(tp0, tp1), Errors.FENCED_STATE_EPOCH, "Fenced exception again");
        assertTrue(partitionCache.isEmpty());
        // Should have 4 fetch recorded (2 fetch and 2 topics) and 3 failures as sp1 was not acquired
        // in first fetch and shall have empty response. Similarly, tp0 should record 2 failures and
        // tp1 should record 1 failure.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(4, 3, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(2, 2, 0, 0), tp1.topic(), new TopicMetrics(2, 1, 0, 0))
        );
    }

    @Test
    public void testListenerRegistration() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1);

        ReplicaManager mockReplicaManager = mock(ReplicaManager.class);
        Partition partition = mockPartition();
        when(mockReplicaManager.getPartitionOrException((TopicPartition) Mockito.any())).thenReturn(partition);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(mockReplicaManager)
            .withBrokerTopicStats(brokerTopicStats)
            .build();

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = sharePartitionManager.fetchMessages(
            groupId, memberId, FETCH_PARAMS, BATCH_OPTIMIZED, 0, MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        assertTrue(future.isDone());
        // Validate that the listener is registered.
        verify(mockReplicaManager, times(2)).maybeAddListener(any(), any());
        // The share partition initialization should error out as further mocks are not provided, the
        // metrics should mark fetch as failed.
        validateBrokerTopicStatsMetrics(
            brokerTopicStats,
            new TopicMetrics(2, 2, 0, 0),
            Map.of(tp0.topic(), new TopicMetrics(1, 1, 0, 0), tp1.topic(), new TopicMetrics(1, 1, 0, 0))
        );
    }

    @Test
    public void testSharePartitionListenerOnFailed() {
        SharePartitionKey sharePartitionKey = new SharePartitionKey("grp",
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0)));
        SharePartitionCache partitionCache = new SharePartitionCache();
        ReplicaManager mockReplicaManager = mock(ReplicaManager.class);

        SharePartitionListener partitionListener = new SharePartitionListener(sharePartitionKey, mockReplicaManager, partitionCache);
        testSharePartitionListener(sharePartitionKey, partitionCache, mockReplicaManager, partitionListener::onFailed);
    }

    @Test
    public void testSharePartitionListenerOnDeleted() {
        SharePartitionKey sharePartitionKey = new SharePartitionKey("grp",
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0)));
        SharePartitionCache partitionCache = new SharePartitionCache();
        ReplicaManager mockReplicaManager = mock(ReplicaManager.class);

        SharePartitionListener partitionListener = new SharePartitionListener(sharePartitionKey, mockReplicaManager, partitionCache);
        testSharePartitionListener(sharePartitionKey, partitionCache, mockReplicaManager, partitionListener::onDeleted);
    }

    @Test
    public void testSharePartitionListenerOnBecomingFollower() {
        SharePartitionKey sharePartitionKey = new SharePartitionKey("grp",
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0)));
        SharePartitionCache partitionCache = new SharePartitionCache();
        ReplicaManager mockReplicaManager = mock(ReplicaManager.class);

        SharePartitionListener partitionListener = new SharePartitionListener(sharePartitionKey, mockReplicaManager, partitionCache);
        testSharePartitionListener(sharePartitionKey, partitionCache, mockReplicaManager, partitionListener::onBecomingFollower);
    }

    @Test
    public void testFetchMessagesRotatePartitions() {
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 1));
        TopicIdPartition tp4 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));
        TopicIdPartition tp5 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));
        TopicIdPartition tp6 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 3));
        List<TopicIdPartition> topicIdPartitions = List.of(tp0, tp1, tp2, tp3, tp4, tp5, tp6);

        sharePartitionManager = Mockito.spy(SharePartitionManagerBuilder.builder().withBrokerTopicStats(brokerTopicStats).build());
        // Capture the arguments passed to processShareFetch.
        ArgumentCaptor<ShareFetch> captor = ArgumentCaptor.forClass(ShareFetch.class);

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 0,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        verify(sharePartitionManager, times(1)).processShareFetch(captor.capture());
        // Verify the partitions rotation, no rotation.
        ShareFetch resultShareFetch = captor.getValue();
        validateRotatedListEquals(resultShareFetch.topicIdPartitions(), topicIdPartitions, 0);

        // Single rotation.
        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 1,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        verify(sharePartitionManager, times(2)).processShareFetch(captor.capture());
        // Verify the partitions rotation, rotate by 1.
        resultShareFetch = captor.getValue();
        validateRotatedListEquals(topicIdPartitions, resultShareFetch.topicIdPartitions(), 1);

        // Rotation by 3, less that the number of partitions.
        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 3,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        verify(sharePartitionManager, times(3)).processShareFetch(captor.capture());
        // Verify the partitions rotation, rotate by 3.
        resultShareFetch = captor.getValue();
        validateRotatedListEquals(topicIdPartitions, resultShareFetch.topicIdPartitions(), 3);

        // Rotation by 12, more than the number of partitions.
        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, 12,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        verify(sharePartitionManager, times(4)).processShareFetch(captor.capture());
        // Verify the partitions rotation, rotate by 5 (12 % 7).
        resultShareFetch = captor.getValue();
        validateRotatedListEquals(topicIdPartitions, resultShareFetch.topicIdPartitions(), 5);
        // Rotation by Integer.MAX_VALUE, boundary test.
        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, BATCH_OPTIMIZED, Integer.MAX_VALUE,
            MAX_FETCH_RECORDS, BATCH_SIZE, topicIdPartitions);
        verify(sharePartitionManager, times(5)).processShareFetch(captor.capture());
        // Verify the partitions rotation, rotate by 1 (2147483647 % 7).
        resultShareFetch = captor.getValue();
        validateRotatedListEquals(topicIdPartitions, resultShareFetch.topicIdPartitions(), 1);
    }

    @Test
    public void testCreateIdleShareFetchTask() throws Exception {
        ReplicaManager replicaManager = mock(ReplicaManager.class);

        MockTimer mockTimer = new MockTimer(time);
        long maxWaitMs = 1000L;

        // Set up the mock to capture and add the timer task
        Mockito.doAnswer(invocation -> {
            TimerTask timerTask = invocation.getArgument(0);
            mockTimer.add(timerTask);
            return null;
        }).when(replicaManager).addShareFetchTimerRequest(Mockito.any(TimerTask.class));

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withTime(time)
            .withTimer(mockTimer)
            .build();

        CompletableFuture<Void> future = sharePartitionManager.createIdleShareFetchTimerTask(maxWaitMs);
        // Future should not be completed immediately
        assertFalse(future.isDone());

        mockTimer.advanceClock(maxWaitMs / 2);
        assertFalse(future.isDone());

        mockTimer.advanceClock((maxWaitMs / 2) + 1);
        // Verify the future is completed after the wait time
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnShareVersionToggle() {
        String groupId = "grp";
        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        // Mock the share partitions corresponding to the topic partitions.
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(
            new SharePartitionKey(groupId, new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0))), sp0
        );
        partitionCache.put(
            new SharePartitionKey(groupId, new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0))), sp1
        );
        partitionCache.put(
            new SharePartitionKey(groupId, new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0))), sp2
        );
        partitionCache.put(
            new SharePartitionKey(groupId, new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo4", 0))), sp3
        );
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .build();
        assertEquals(4, partitionCache.size());
        sharePartitionManager.onShareVersionToggle(ShareVersion.SV_0, false);
        // Because we are toggling to a share version which does not support share groups, the cache inside share partitions must be cleared.
        assertEquals(0, partitionCache.size());
        //Check if all share partitions have been fenced.
        Mockito.verify(sp0).markFenced();
        Mockito.verify(sp1).markFenced();
        Mockito.verify(sp2).markFenced();
        Mockito.verify(sp3).markFenced();
    }

    @Test
    public void testOnShareVersionToggleWhenEnabledFromConfig() {
        SharePartition sp0 = mock(SharePartition.class);
        // Mock the share partitions corresponding to the topic partitions.
        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.put(
            new SharePartitionKey("grp", new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))), sp0
        );
        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCache(partitionCache)
            .build();
        assertEquals(1, partitionCache.size());
        sharePartitionManager.onShareVersionToggle(ShareVersion.SV_0, true);
        // Though share version is toggled to off, but it's enabled from config, hence the cache should not be cleared.
        assertEquals(1, partitionCache.size());
        Mockito.verify(sp0, times(0)).markFenced();
    }

    @Test
    public void testShareGroupListener() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        ShareSessionCache cache = new ShareSessionCache(10);
        cache.maybeCreateSession(groupId, memberId1, new ImplicitLinkedHashCollection<>(), CONNECTION_ID);
        cache.maybeCreateSession(groupId, memberId2, new ImplicitLinkedHashCollection<>(), "id-2");

        SharePartitionCache partitionCache = new SharePartitionCache();
        partitionCache.computeIfAbsent(new SharePartitionKey(groupId, tp0), k -> sp0);
        partitionCache.computeIfAbsent(new SharePartitionKey(groupId, tp1), k -> sp1);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .build();

        assertEquals(2, cache.size());
        assertEquals(2, partitionCache.size());

        // Invoke listeners by simulating connection disconnect for memberId1.
        cache.connectionDisconnectListener().onDisconnect(CONNECTION_ID);
        // Session cache should remove the memberId1.
        assertEquals(1, cache.size());
        // Partition cache should not remove the share partitions as the group is not empty.
        assertEquals(2, partitionCache.size());
        assertNotNull(cache.get(new ShareSessionKey(groupId, memberId2)));

        // Invoke listeners by simulating connection disconnect for memberId2.
        cache.connectionDisconnectListener().onDisconnect("id-2");
        // Session cache should remove the memberId2.
        assertEquals(0, cache.size());
        // Partition cache should remove the share partitions as the group is empty.
        assertEquals(0, partitionCache.size());

        Mockito.verify(sp0, times(1)).markFenced();
        Mockito.verify(sp1, times(1)).markFenced();
        Mockito.verify(mockReplicaManager, times(2)).removeListener(any(), any());
    }

    @Test
    public void testShareGroupListenerWithEmptyCache() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        String memberId1 = Uuid.randomUuid().toString();

        SharePartition sp0 = mock(SharePartition.class);

        ShareSessionCache cache = new ShareSessionCache(10);
        cache.maybeCreateSession(groupId, memberId1, new ImplicitLinkedHashCollection<>(), CONNECTION_ID);

        SharePartitionCache partitionCache = spy(new SharePartitionCache());
        partitionCache.computeIfAbsent(new SharePartitionKey(groupId, tp0), k -> sp0);

        sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .withPartitionCache(partitionCache)
            .withReplicaManager(mockReplicaManager)
            .build();

        assertEquals(1, cache.size());
        assertEquals(1, partitionCache.size());

        // Clean up share session and partition cache.
        sharePartitionManager.onShareVersionToggle(ShareVersion.SV_0, false);
        assertEquals(0, cache.size());
        assertEquals(0, partitionCache.size());

        Mockito.verify(sp0, times(1)).markFenced();
        Mockito.verify(mockReplicaManager, times(1)).removeListener(any(), any());
        Mockito.verify(partitionCache, times(0)).topicIdPartitionsForGroup(groupId);

        // Invoke listeners by simulating connection disconnect for member. As the group is empty,
        // hence onGroupEmpty method should be invoked and should complete without any exception.
        cache.connectionDisconnectListener().onDisconnect(CONNECTION_ID);
        // Verify that the listener is called for the group.
        Mockito.verify(partitionCache, times(1)).topicIdPartitionsForGroup(groupId);
    }

    private Timer systemTimerReaper() {
        return new SystemTimerReaper(
            TIMER_NAME_PREFIX + "-test-reaper",
            new SystemTimer(TIMER_NAME_PREFIX + "-test-timer"));
    }

    private void assertNoReaperThreadsPendingClose() throws InterruptedException {
        TestUtils.waitForCondition(
            () -> Thread.getAllStackTraces().keySet().stream().noneMatch(t -> t.getName().contains(TIMER_NAME_PREFIX)),
            "Found unexpected reaper threads with name containing: " + TIMER_NAME_PREFIX);
    }

    private void testSharePartitionListener(
        SharePartitionKey sharePartitionKey,
        SharePartitionCache partitionCache,
        ReplicaManager mockReplicaManager,
        Consumer<TopicPartition> listenerConsumer
    ) {
        // Add another share partition to the cache.
        TopicPartition tp = new TopicPartition("foo", 1);
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        SharePartitionKey spk = new SharePartitionKey("grp", tpId);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        partitionCache.put(sharePartitionKey, sp0);
        partitionCache.put(spk, sp1);

        // Invoke listener for first share partition.
        listenerConsumer.accept(sharePartitionKey.topicIdPartition().topicPartition());

        // Validate that the share partition is removed from the cache.
        assertEquals(1, partitionCache.size());
        assertFalse(partitionCache.containsKey(sharePartitionKey));
        verify(sp0, times(1)).markFenced();
        verify(mockReplicaManager, times(1)).removeListener(any(), any());

        // Invoke listener for non-matching share partition.
        listenerConsumer.accept(tp);
        // The non-matching share partition should not be removed as the listener is attached to a different topic partition.
        assertEquals(1, partitionCache.size());
        verify(sp1, times(0)).markFenced();
        // Verify the remove listener is not called for the second share partition.
        verify(mockReplicaManager, times(1)).removeListener(any(), any());
    }

    private ShareFetchResponseData.PartitionData noErrorShareFetchResponse() {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0);
    }

    private ShareFetchResponseData.PartitionData errorShareFetchResponse(Short errorCode) {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0).setErrorCode(errorCode);
    }

    private void mockUpdateAndGenerateResponseData(ShareFetchContext context, String groupId, String memberId) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> data = new LinkedHashMap<>();
        if (context.getClass() == ShareSessionContext.class) {
            ShareSessionContext shareSessionContext = (ShareSessionContext) context;
            if (!shareSessionContext.isSubsequent()) {
                shareSessionContext.shareFetchData().forEach(topicIdPartition -> data.put(topicIdPartition,
                        topicIdPartition.topic() == null ? errorShareFetchResponse(Errors.UNKNOWN_TOPIC_ID.code()) :
                                noErrorShareFetchResponse()));
            } else {
                synchronized (shareSessionContext.session()) {
                    shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(),
                            new TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                        data.put(topicIdPartition, topicIdPartition.topic() == null ? errorShareFetchResponse(Errors.UNKNOWN_TOPIC_ID.code()) :
                                noErrorShareFetchResponse());
                    });
                }
            }
        }
        context.updateAndGenerateResponseData(groupId, memberId, data);
    }

    private void assertPartitionsPresent(ShareSessionContext context, List<TopicIdPartition> partitions) {
        Set<TopicIdPartition> partitionsInContext = new HashSet<>();
        if (!context.isSubsequent()) {
            partitionsInContext.addAll(context.shareFetchData());
        } else {
            context.session().partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                partitionsInContext.add(topicIdPartition);
            });
        }
        Set<TopicIdPartition> partitionsSet = new HashSet<>(partitions);
        assertEquals(partitionsSet, partitionsInContext);
    }

    private void assertErroneousAndValidTopicIdPartitions(
        ErroneousAndValidPartitionData erroneousAndValidPartitionData,
        List<TopicIdPartition> expectedErroneous, List<TopicIdPartition> expectedValid) {
        Set<TopicIdPartition> expectedErroneousSet = new HashSet<>(expectedErroneous);
        Set<TopicIdPartition> expectedValidSet = new HashSet<>(expectedValid);
        Set<TopicIdPartition> actualErroneousPartitions = new HashSet<>();
        erroneousAndValidPartitionData.erroneous().forEach((topicIdPartition, partitionData) ->
                actualErroneousPartitions.add(topicIdPartition));
        Set<TopicIdPartition> actualValidPartitions = new HashSet<>(erroneousAndValidPartitionData.validTopicIdPartitions());
        assertEquals(expectedErroneousSet, actualErroneousPartitions);
        assertEquals(expectedValidSet, actualValidPartitions);
    }

    private Partition mockPartition() {
        Partition partition = mock(Partition.class);
        when(partition.isLeader()).thenReturn(true);
        when(partition.getLeaderEpoch()).thenReturn(1);

        return partition;
    }

    private void validateShareFetchFutureException(CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        TopicIdPartition topicIdPartition, Errors error) {
        validateShareFetchFutureException(future, List.of(topicIdPartition), error, null);
    }

    private void validateShareFetchFutureException(CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        TopicIdPartition topicIdPartition, Errors error, String message) {
        validateShareFetchFutureException(future, List.of(topicIdPartition), error, message);
    }

    private void validateShareFetchFutureException(CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        List<TopicIdPartition> topicIdPartitions, Errors error, String message) {
        assertFalse(future.isCompletedExceptionally());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = future.join();
        assertEquals(topicIdPartitions.size(), result.size());
        topicIdPartitions.forEach(topicIdPartition -> {
            assertTrue(result.containsKey(topicIdPartition));
            assertEquals(topicIdPartition.partition(), result.get(topicIdPartition).partitionIndex());
            assertEquals(error.code(), result.get(topicIdPartition).errorCode());
            assertEquals(message, result.get(topicIdPartition).errorMessage());
        });
    }

    private void mockFetchOffsetForTimestamp(ReplicaManager replicaManager) {
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 0L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    private void validateBrokerTopicStatsMetrics(
        BrokerTopicStats brokerTopicStats,
        TopicMetrics expectedAllTopicMetrics,
        Map<String, TopicMetrics> expectedTopicMetrics
    ) {
        if (expectedAllTopicMetrics != null) {
            assertEquals(expectedAllTopicMetrics.totalShareFetchRequestCount, brokerTopicStats.allTopicsStats().totalShareFetchRequestRate().count());
            assertEquals(expectedAllTopicMetrics.failedShareFetchRequestCount, brokerTopicStats.allTopicsStats().failedShareFetchRequestRate().count());
            assertEquals(expectedAllTopicMetrics.totalShareAcknowledgementRequestCount, brokerTopicStats.allTopicsStats().totalShareAcknowledgementRequestRate().count());
            assertEquals(expectedAllTopicMetrics.failedShareAcknowledgementRequestCount, brokerTopicStats.allTopicsStats().failedShareAcknowledgementRequestRate().count());
        }
        // Validate tracked topic metrics.
        assertEquals(expectedTopicMetrics.size(), brokerTopicStats.numTopics());
        expectedTopicMetrics.forEach((topic, metrics) -> {
            BrokerTopicMetrics topicMetrics = brokerTopicStats.topicStats(topic);
            assertEquals(metrics.totalShareFetchRequestCount, topicMetrics.totalShareFetchRequestRate().count());
            assertEquals(metrics.failedShareFetchRequestCount, topicMetrics.failedShareFetchRequestRate().count());
            assertEquals(metrics.totalShareAcknowledgementRequestCount, topicMetrics.totalShareAcknowledgementRequestRate().count());
            assertEquals(metrics.failedShareAcknowledgementRequestCount, topicMetrics.failedShareAcknowledgementRequestRate().count());
        });
    }

    static Seq<Tuple2<TopicIdPartition, LogReadResult>> buildLogReadResult(List<TopicIdPartition> topicIdPartitions) {
        List<Tuple2<TopicIdPartition, LogReadResult>> logReadResults = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> logReadResults.add(new Tuple2<>(topicIdPartition, new LogReadResult(
            new FetchDataInfo(new LogOffsetMetadata(0, 0, 0), MemoryRecords.withRecords(
                    Compression.NONE, new SimpleRecord("test-key".getBytes(), "test-value".getBytes()))),
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ))));
        return CollectionConverters.asScala(logReadResults).toSeq();
    }

    @SuppressWarnings("unchecked")
    static void mockReplicaManagerDelayedShareFetch(ReplicaManager replicaManager,
                                                    DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory) {
        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            DelayedShareFetchKey key = (DelayedShareFetchKey) args[0];
            delayedShareFetchPurgatory.checkAndComplete(key);
            return null;
        }).when(replicaManager).completeDelayedShareFetchRequest(any(DelayedShareFetchKey.class));

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            DelayedShareFetch operation = (DelayedShareFetch) args[0];
            List<DelayedOperationKey> keys = (List<DelayedOperationKey>) args[1];
            delayedShareFetchPurgatory.tryCompleteElseWatch(operation, keys);
            return null;
        }).when(replicaManager).addDelayedShareFetchRequest(any(), any());
    }

    private record TopicMetrics(
        long totalShareFetchRequestCount,
        long failedShareFetchRequestCount,
        long totalShareAcknowledgementRequestCount,
        long failedShareAcknowledgementRequestCount
    ) { }

    static class SharePartitionManagerBuilder {
        private final Persister persister = new NoOpStatePersister();
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private Time time = new MockTime();
        private ShareSessionCache cache = new ShareSessionCache(10);
        private SharePartitionCache partitionCache = new SharePartitionCache();
        private Timer timer = new MockTimer();
        private ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        private BrokerTopicStats brokerTopicStats;

        private SharePartitionManagerBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        private SharePartitionManagerBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private SharePartitionManagerBuilder withCache(ShareSessionCache cache) {
            this.cache = cache;
            return this;
        }

        SharePartitionManagerBuilder withPartitionCache(SharePartitionCache partitionCache) {
            this.partitionCache = partitionCache;
            return this;
        }

        private SharePartitionManagerBuilder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        private SharePartitionManagerBuilder withShareGroupMetrics(ShareGroupMetrics shareGroupMetrics) {
            this.shareGroupMetrics = shareGroupMetrics;
            return this;
        }

        private SharePartitionManagerBuilder withBrokerTopicStats(BrokerTopicStats brokerTopicStats) {
            this.brokerTopicStats = brokerTopicStats;
            return this;
        }

        public static SharePartitionManagerBuilder builder() {
            return new SharePartitionManagerBuilder();
        }

        public SharePartitionManager build() {
            return new SharePartitionManager(replicaManager,
                time,
                cache,
                partitionCache,
                DEFAULT_RECORD_LOCK_DURATION_MS,
                timer,
                MAX_DELIVERY_COUNT,
                MAX_IN_FLIGHT_MESSAGES,
                REMOTE_FETCH_MAX_WAIT_MS,
                persister,
                mock(GroupConfigManager.class),
                shareGroupMetrics,
                brokerTopicStats
            );
        }
    }
}
