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
import kafka.log.OffsetResultHolder;
import kafka.server.LogReadResult;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidShareSessionEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
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
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.persister.NoOpShareStatePersister;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.session.ShareSession;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.share.session.ShareSessionKey;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static kafka.server.share.DelayedShareFetchTest.mockTopicIdPartitionToReturnDataEqualToMinBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@Timeout(120)
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class SharePartitionManagerTest {

    private static final int DEFAULT_RECORD_LOCK_DURATION_MS = 30000;
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    private static final short MAX_FETCH_RECORDS = 500;
    private static final int DELAYED_SHARE_FETCH_MAX_WAIT_MS = 2000;
    private static final int DELAYED_SHARE_FETCH_TIMEOUT_MS = 3000;
    private static final FetchParams FETCH_PARAMS = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(),
        FetchRequest.ORDINARY_CONSUMER_ID, -1, DELAYED_SHARE_FETCH_MAX_WAIT_MS,
        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty(), true);

    static final int PARTITION_MAX_BYTES = 40000;
    static final int DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL = 1000;

    private Timer mockTimer;
    private ReplicaManager mockReplicaManager;

    private static final List<TopicIdPartition> EMPTY_PART_LIST = Collections.unmodifiableList(new ArrayList<>());

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("sharePartitionManagerTestReaper",
            new SystemTimer("sharePartitionManagerTestTimer"));
        mockReplicaManager = mock(ReplicaManager.class);
        Partition partition = mockPartition();
        when(mockReplicaManager.getPartitionOrException(Mockito.any())).thenReturn(partition);
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    @Test
    public void testNewContextReturnsFinalContextWithoutRequestData() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), PARTITION_MAX_BYTES));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), PARTITION_MAX_BYTES));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(memberId, ShareRequestMetadata.FINAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.emptyList(), reqMetadata2, true);
        assertEquals(FinalContext.class, context2.getClass());
    }

    @Test
    public void testNewContextReturnsFinalContextWithRequestData() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), PARTITION_MAX_BYTES));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), PARTITION_MAX_BYTES));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(memberId, ShareRequestMetadata.FINAL_EPOCH);

        // shareFetch is not empty, but the maxBytes of topic partition is 0, which means this is added only for acknowledgements.
        // New context should be created successfully
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData3 = Collections.singletonMap(new TopicIdPartition(tpId1, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(tpId1, 0));
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData3, Collections.emptyList(), reqMetadata2, true);
        assertEquals(FinalContext.class, context2.getClass());
    }

    @Test
    public void testNewContextReturnsFinalContextError() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));

        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), PARTITION_MAX_BYTES));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), PARTITION_MAX_BYTES));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(memberId, ShareRequestMetadata.FINAL_EPOCH);

        // shareFetch is not empty and the maxBytes of topic partition is not 0, which means this is trying to fetch on a Final request.
        // New context should throw an error
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData3 = Collections.singletonMap(new TopicIdPartition(tpId1, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(tpId1, PARTITION_MAX_BYTES));
        assertThrows(InvalidRequestException.class,
                () -> sharePartitionManager.newContext(groupId, reqData3, Collections.emptyList(), reqMetadata2, true));
    }

    @Test
    public void testNewContext() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
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
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));


        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2, false);
        assertEquals(ShareSessionContext.class, context2.getClass());
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        ((ShareSessionContext) context2).shareFetchData().forEach((topicIdPartition, sharePartitionData) -> {
            assertTrue(reqData2.containsKey(topicIdPartition));
            assertEquals(reqData2.get(topicIdPartition), sharePartitionData);
        });

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(memberId4, 1), true));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context5.getClass());
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        ShareSessionContext shareSessionContext5 = (ShareSessionContext) context5;
        synchronized (shareSessionContext5.session()) {
            shareSessionContext5.session().partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                ShareFetchRequest.SharePartitionData data = cachedSharePartition.reqData();
                assertTrue(reqData2.containsKey(topicIdPartition));
                assertEquals(reqData2.get(topicIdPartition), data);
            });
        }
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(0, resp5.responseData(topicNames).size());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 2), true);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata2.memberId(), ShareRequestMetadata.FINAL_EPOCH), true);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());

        // Close the session.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseResponse =
            sharePartitionManager.releaseSession(groupId, reqMetadata2.memberId().toString());
        assertTrue(releaseResponse.isDone());
        assertFalse(releaseResponse.isCompletedExceptionally());
        assertEquals(0, cache.size());
    }

    @Test
    public void testShareSessionExpiration() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(2, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new share session, session 1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session1req = new LinkedHashMap<>();
        session1req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session1req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        String groupId = "grp";
        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        ShareFetchContext session1context = sharePartitionManager.newContext(groupId, session1req, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(session1context.getClass(), ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData1.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session1resp = session1context.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, session1resp.error());
        assertEquals(2, session1resp.responseData(topicNames).size());

        ShareSessionKey session1Key = new ShareSessionKey(groupId, reqMetadata1.memberId());
        // check share session entered into cache
        assertNotNull(cache.get(session1Key));

        time.sleep(500);

        // Create a second new share session
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session2req = new LinkedHashMap<>();
        session2req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session2req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        ShareFetchContext session2context = sharePartitionManager.newContext(groupId, session2req, EMPTY_PART_LIST, reqMetadata2, false);
        assertEquals(session2context.getClass(), ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData2.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session2resp = session2context.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, session2resp.error());
        assertEquals(2, session2resp.responseData(topicNames).size());

        ShareSessionKey session2Key = new ShareSessionKey(groupId, reqMetadata2.memberId());

        // both newly created entries are present in cache
        assertNotNull(cache.get(session1Key));
        assertNotNull(cache.get(session2Key));

        time.sleep(500);

        // Create a subsequent share fetch context for session 1
        ShareFetchContext session1context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata1.memberId(), 1), true);
        assertEquals(session1context2.getClass(), ShareSessionContext.class);

        // total sleep time will now be large enough that share session 1 will be evicted if not correctly touched
        time.sleep(501);

        // create one final share session to test that the least recently used entry is evicted
        // the second share session should be evicted because the first share session was incrementally fetched
        // more recently than the second session was created
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session3req = new LinkedHashMap<>();
        session3req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session3req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        ShareRequestMetadata reqMetadata3 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        ShareFetchContext session3context = sharePartitionManager.newContext(groupId, session3req, EMPTY_PART_LIST, reqMetadata3, false);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData3.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session3resp = session3context.updateAndGenerateResponseData(groupId, reqMetadata3.memberId(), respData3);
        assertEquals(Errors.NONE, session3resp.error());
        assertEquals(2, session3resp.responseData(topicNames).size());

        ShareSessionKey session3Key = new ShareSessionKey(groupId, reqMetadata3.memberId());

        assertNotNull(cache.get(session1Key));
        assertNull(cache.get(session2Key), "share session 2 should have been evicted by latest share session, " +
                "as share session 1 was used more recently");
        assertNotNull(cache.get(session3Key));
    }

    @Test
    public void testSubsequentShareSession() {
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        // Create a new share session with foo-0 and foo-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        String groupId = "grp";
        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp0.partition()));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent fetch request that removes foo-0 and adds bar-0
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = Collections.singletonMap(
                tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(tp0);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, removed2,
                new ShareRequestMetadata(reqMetadata1.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context2.getClass());

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

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(1, resp2.data().responses().size());
        assertEquals(barId, resp2.data().responses().get(0).topicId());
        assertEquals(1, resp2.data().responses().get(0).partitions().size());
        assertEquals(0, resp2.data().responses().get(0).partitions().get(0).partitionIndex());
        assertEquals(1, resp2.responseData(topicNames).size());
    }

    @Test
    public void testZeroSizeShareSession() {
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new share session with foo-0 and foo-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        reqData1.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        String groupId = "grp";
        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData1.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share request that removes foo-0 and foo-1
        // Verify that the previous share session was closed.
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(foo0);
        removed2.add(foo1);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), removed2,
                new ShareRequestMetadata(reqMetadata1.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context2.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertTrue(resp2.responseData(topicNames).isEmpty());
        assertEquals(1, cache.size());
    }

    @Test
    public void testToForgetPartitions() {
        String groupId = "grp";
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);

        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo, new ShareFetchRequest.SharePartitionData(foo.topicId(), 100));
        reqData1.put(bar, new ShareFetchRequest.SharePartitionData(bar.topicId(), 100));


        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertPartitionsPresent((ShareSessionContext) context1, Arrays.asList(foo, bar));

        mockUpdateAndGenerateResponseData(context1, groupId, reqMetadata1.memberId());

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(foo),
                new ShareRequestMetadata(reqMetadata1.memberId(), 1), true);

        // So foo is removed but not the others.
        assertPartitionsPresent((ShareSessionContext) context2, Collections.singletonList(bar));

        mockUpdateAndGenerateResponseData(context2, groupId, reqMetadata1.memberId());

        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(bar),
                new ShareRequestMetadata(reqMetadata1.memberId(), 2), true);
        assertPartitionsPresent((ShareSessionContext) context3, Collections.emptyList());
    }

    // This test simulates a share session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
    // -- as though the topic is deleted and recreated.
    @Test
    public void testShareSessionUpdateTopicIdsBrokerSide() {
        String groupId = "grp";
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 1));

        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");

        // Create a new share session with foo-0 and bar-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo, new ShareFetchRequest.SharePartitionData(foo.topicId(), 100));
        reqData1.put(bar, new ShareFetchRequest.SharePartitionData(bar.topicId(), 100));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);

        assertEquals(ShareSessionContext.class, context1.getClass());
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(bar, new ShareFetchResponseData.PartitionData().setPartitionIndex(bar.partition()));
        respData1.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share fetch request as though no topics changed.
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata1.memberId(), 1), true);

        assertEquals(ShareSessionContext.class, context2.getClass());
        assertTrue(((ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
        respData2.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.INCONSISTENT_TOPIC_ID.code()));
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        // We should have the inconsistent topic ID error on the partition
        assertEquals(Errors.INCONSISTENT_TOPIC_ID.code(), resp2.responseData(topicNames).get(foo).errorCode());
    }

    @Test
    public void testGetErroneousAndValidTopicIdPartitions() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Uuid tpId0 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tpNull1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(null, 0));
        TopicIdPartition tpNull2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(null, 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));
        reqData2.put(tpNull1, new ShareFetchRequest.SharePartitionData(tpNull1.topicId(), 100));


        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2, false);
        assertEquals(ShareSessionContext.class, context2.getClass());
        assertFalse(((ShareSessionContext) context2).isSubsequent());
        assertErroneousAndValidTopicIdPartitions(context2.getErroneousAndValidTopicIdPartitions(), Collections.singletonList(tpNull1), Arrays.asList(tp0, tp1));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        respData2.put(tpNull1, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId, reqMetadata2.memberId());

        // Check for throttled response
        ShareFetchResponse resp2Throttle = context2.throttleResponse(100);
        assertEquals(Errors.NONE, resp2Throttle.error());
        assertEquals(100, resp2Throttle.throttleTimeMs());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test trying to create a new session with a non-existent session key
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(Uuid.randomUuid(), 1), true));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context5.getClass());
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        assertErroneousAndValidTopicIdPartitions(context5.getErroneousAndValidTopicIdPartitions(), Collections.singletonList(tpNull1), Arrays.asList(tp0, tp1));

        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test generating a throttled response for a subsequent share session
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = Collections.singletonMap(
                tpNull2, new ShareFetchRequest.SharePartitionData(tpNull2.topicId(), 100));
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 2), true);
        // Check for throttled response
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        assertErroneousAndValidTopicIdPartitions(context7.getErroneousAndValidTopicIdPartitions(), Arrays.asList(tpNull1, tpNull2), Arrays.asList(tp0, tp1));

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata2.memberId(), ShareRequestMetadata.FINAL_EPOCH), true);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        assertErroneousAndValidTopicIdPartitions(context8.getErroneousAndValidTopicIdPartitions(), Collections.emptyList(), Collections.emptyList());
        // Check for throttled response
        ShareFetchResponse resp8 = context8.throttleResponse(100);
        assertEquals(Errors.NONE, resp8.error());
        assertEquals(100, resp8.throttleTimeMs());

        // Close the session.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseResponse =
            sharePartitionManager.releaseSession(groupId, reqMetadata2.memberId().toString());
        assertTrue(releaseResponse.isDone());
        assertFalse(releaseResponse.isCompletedExceptionally());
        assertEquals(0, cache.size());
    }

    @Test
    public void testShareFetchContextResponseSize() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
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
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        // For response size expected value calculation
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        short version = ApiKeys.SHARE_FETCH.latestVersion();

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(Uuid.randomUuid(), ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2, false);
        assertEquals(ShareSessionContext.class, context2.getClass());
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize2 = context2.responseSize(respData2, version);
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp2.data().size(objectSerializationCache, version), respSize2);

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(memberId4, 1), true));

        // Continue the first share session we created.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = Collections.singletonMap(
                tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context5.getClass());
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        respData5.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        int respSize5 = context5.responseSize(respData5, version);
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData5);
        assertEquals(Errors.NONE, resp5.error());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp5.data().size(objectSerializationCache, version), respSize5);

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 5), true));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey2.memberId(), 2), true);

        int respSize7 = context7.responseSize(respData2, version);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + new ShareFetchResponseData().size(objectSerializationCache, version), respSize7);

        // Get the final share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata2.memberId(), ShareRequestMetadata.FINAL_EPOCH), true);
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(1, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize8 = context8.responseSize(respData8, version);
        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp8.data().size(objectSerializationCache, version), respSize8);
    }

    @Test
    public void testCachedTopicPartitionsWithNoTopicPartitions() {
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();

        List<TopicIdPartition> result = sharePartitionManager.cachedTopicIdPartitionsInShareSession("grp", Uuid.randomUuid());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testCachedTopicPartitionsForValidShareSessions() {
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        Uuid memberId2 = Uuid.randomUuid();

        // Create a new share session with an initial share fetch request.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        ShareRequestMetadata reqMetadata1 = new ShareRequestMetadata(memberId1, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1, false);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertFalse(((ShareSessionContext) context1).isSubsequent());

        ShareSessionKey shareSessionKey1 = new ShareSessionKey(groupId,
                reqMetadata1.memberId());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());

        assertEquals(new HashSet<>(Arrays.asList(tp0, tp1)),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Create a new share session with an initial share fetch request.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = Collections.singletonMap(
                tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));

        ShareRequestMetadata reqMetadata2 = new ShareRequestMetadata(memberId2, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2, false);
        assertEquals(ShareSessionContext.class, context2.getClass());
        assertFalse(((ShareSessionContext) context2).isSubsequent());

        ShareSessionKey shareSessionKey2 = new ShareSessionKey(groupId,
                reqMetadata2.memberId());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());

        assertEquals(Collections.singletonList(tp2), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Continue the first share session we created.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData3 = Collections.singletonMap(
                tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, reqData3, EMPTY_PART_LIST,
                new ShareRequestMetadata(shareSessionKey1.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context3.getClass());
        assertTrue(((ShareSessionContext) context3).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        ShareFetchResponse resp3 = context3.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData3);
        assertEquals(Errors.NONE, resp3.error());

        assertEquals(new HashSet<>(Arrays.asList(tp0, tp1, tp2)),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Continue the second session we created.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData4 = Collections.singletonMap(
                tp3, new ShareFetchRequest.SharePartitionData(tp3.topicId(), 100));
        ShareFetchContext context4 = sharePartitionManager.newContext(groupId, reqData4, Collections.singletonList(tp2),
                new ShareRequestMetadata(shareSessionKey2.memberId(), 1), true);
        assertEquals(ShareSessionContext.class, context4.getClass());
        assertTrue(((ShareSessionContext) context4).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData4 = new LinkedHashMap<>();
        respData4.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        ShareFetchResponse resp4 = context4.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData4);
        assertEquals(Errors.NONE, resp4.error());

        assertEquals(Collections.singletonList(tp3), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Get the final share session.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareRequestMetadata(reqMetadata1.memberId(), ShareRequestMetadata.FINAL_EPOCH), true);
        assertEquals(FinalContext.class, context5.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData5);
        assertEquals(Errors.NONE, resp5.error());

        assertFalse(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1).isEmpty());

        // Close the first session.
        sharePartitionManager.releaseSession(groupId, reqMetadata1.memberId().toString());
        assertTrue(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1).isEmpty());

        // Continue the second share session .
        ShareFetchContext context6 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(tp3),
                new ShareRequestMetadata(shareSessionKey2.memberId(), 2), true);
        assertEquals(ShareSessionContext.class, context6.getClass());
        assertTrue(((ShareSessionContext) context6).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData6 = new LinkedHashMap<>();
        ShareFetchResponse resp6 = context6.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData6);
        assertEquals(Errors.NONE, resp6.error());

        assertEquals(Collections.emptyList(), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));
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
        assertNotEquals(sharePartitionKey1, null);
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
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp3, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp4, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp5, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp6, PARTITION_MAX_BYTES);

        mockFetchOffsetForTimestamp(mockReplicaManager);

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(0L).thenReturn(100L);
        Metrics metrics = new Metrics();
        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp0, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp2, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp3, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp4, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp5, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp6, 1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(mockReplicaManager)
            .withTime(time)
            .withMetrics(metrics)
            .withTimer(mockTimer)
            .build();

        doAnswer(invocation -> buildLogReadResult(partitionMaxBytes.keySet())).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, partitionMaxBytes);
        Mockito.verify(mockReplicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, partitionMaxBytes);
        Mockito.verify(mockReplicaManager, times(2)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, partitionMaxBytes);
        Mockito.verify(mockReplicaManager, times(3)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        Map<MetricName, Consumer<Double>> expectedMetrics = new HashMap<>();
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.PARTITION_LOAD_TIME_AVG, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME),
                val -> assertEquals(val.intValue(), (int) 100.0 / 7, SharePartitionManager.ShareGroupMetrics.PARTITION_LOAD_TIME_AVG)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.PARTITION_LOAD_TIME_MAX, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME),
                val -> assertEquals(val, 100.0, SharePartitionManager.ShareGroupMetrics.PARTITION_LOAD_TIME_MAX)
        );
        expectedMetrics.forEach((metric, test) -> {
            assertTrue(metrics.metrics().containsKey(metric));
            test.accept((Double) metrics.metrics().get(metric).metricValue());
        });
    }

    @Test
    public void testMultipleConcurrentShareFetches() throws InterruptedException {

        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(barId, new TopicPartition("bar", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp3, PARTITION_MAX_BYTES);

        final Time time = new MockTime(0, System.currentTimeMillis(), 0);

        mockFetchOffsetForTimestamp(mockReplicaManager);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp0, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp2, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp3, 1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withTime(time)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 1, (long) 15, (long) 6, (long) 30, (long) 25);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 1, (long) 18, (long) 5);
        when(sp2.nextFetchOffset()).thenReturn((long) 10, (long) 25, (long) 26);
        when(sp3.nextFetchOffset()).thenReturn((long) 20, (long) 15, (long) 23, (long) 16);

        doAnswer(invocation -> {
            assertEquals(1, sp0.nextFetchOffset());
            assertEquals(4, sp1.nextFetchOffset());
            assertEquals(10, sp2.nextFetchOffset());
            assertEquals(20, sp3.nextFetchOffset());
            return buildLogReadResult(partitionMaxBytes.keySet());
        }).doAnswer(invocation -> {
            assertEquals(15, sp0.nextFetchOffset());
            assertEquals(1, sp1.nextFetchOffset());
            assertEquals(25, sp2.nextFetchOffset());
            assertEquals(15, sp3.nextFetchOffset());
            return buildLogReadResult(partitionMaxBytes.keySet());
        }).doAnswer(invocation -> {
            assertEquals(6, sp0.nextFetchOffset());
            assertEquals(18, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(23, sp3.nextFetchOffset());
            return buildLogReadResult(partitionMaxBytes.keySet());
        }).doAnswer(invocation -> {
            assertEquals(30, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            return buildLogReadResult(partitionMaxBytes.keySet());
        }).doAnswer(invocation -> {
            assertEquals(25, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            return buildLogReadResult(partitionMaxBytes.keySet());
        }).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i != threadCount; ++i) {
                executorService.submit(() -> {
                    sharePartitionManager.fetchMessages(groupId, memberId1.toString(), FETCH_PARAMS, partitionMaxBytes);
                });
                // We are blocking the main thread at an interval of 10 threads so that the currently running executorService threads can complete.
                if (i % 10 == 0)
                    executorService.awaitTermination(50, TimeUnit.MILLISECONDS);
            }
        } finally {
            if (!executorService.awaitTermination(50, TimeUnit.MILLISECONDS))
                executorService.shutdown();
        }
        // We are checking the number of replicaManager readFromLog() calls
        Mockito.verify(mockReplicaManager, atMost(100)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(mockReplicaManager, atLeast(10)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
    }

    @Test
    public void testReplicaManagerFetchShouldNotProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        Mockito.verify(mockReplicaManager, times(0)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = future.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testReplicaManagerFetchShouldProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        mockFetchOffsetForTimestamp(mockReplicaManager);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp0, 1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        doAnswer(invocation -> buildLogReadResult(partitionMaxBytes.keySet())).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        // Since the nextFetchOffset does not point to endOffset + 1, i.e. some of the records in the cachedState are AVAILABLE,
        // even though the maxInFlightMessages limit is exceeded, replicaManager.readFromLog should be called
        Mockito.verify(mockReplicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
    }

    @Test
    public void testCloseSharePartitionManager() throws Exception {
        Timer timer = Mockito.mock(SystemTimerReaper.class);
        Persister persister = Mockito.mock(Persister.class);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withTimer(timer).withShareGroupPersister(persister).build();

        // Verify that 0 calls are made to timer.close() and persister.stop().
        Mockito.verify(timer, times(0)).close();
        Mockito.verify(persister, times(0)).stop();
        // Closing the sharePartitionManager closes timer object in sharePartitionManager.
        sharePartitionManager.close();
        // Verify that the timer object in sharePartitionManager is closed by checking the calls to timer.close() and persister.stop().
        Mockito.verify(timer, times(1)).close();
    }

    @Test
    public void testReleaseSessionSuccess() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("baz", 4));

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId.toString()))).thenReturn(CompletableFuture.completedFuture(null));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(memberId.toString()))).thenReturn(FutureUtils.failedFuture(
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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .withPartitionCacheMap(partitionCacheMap)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId.toString());
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.INVALID_RECORD_STATE.code(), result.get(tp2).errorCode());
        // tp3 was not a part of partitionCacheMap.
        assertEquals(4, result.get(tp3).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp3).errorCode());
    }

    @Test
    public void testReleaseSessionWithIncorrectGroupId() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);

        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap = new ImplicitLinkedHashCollection<>(3);
        partitionMap.add(new CachedSharePartition(tp1));
        when(shareSession.partitionMap()).thenReturn(partitionMap);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        // Calling releaseSession with incorrect groupId.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession("grp-2", memberId.toString());
        assertTrue(resultFuture.isDone());
        assertTrue(resultFuture.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, resultFuture::get);
        assertInstanceOf(ShareSessionNotFoundException.class, exception.getCause());
    }

    @Test
    public void testReleaseSessionWithIncorrectMemberId() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        TopicIdPartition tp1 = new TopicIdPartition(memberId, new TopicPartition("foo", 0));

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        // Member with random Uuid so that it does not match the memberId.
        when(cache.get(new ShareSessionKey(groupId, Uuid.randomUuid()))).thenReturn(shareSession);

        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap = new ImplicitLinkedHashCollection<>(3);
        partitionMap.add(new CachedSharePartition(tp1));
        when(shareSession.partitionMap()).thenReturn(partitionMap);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId.toString());
        assertTrue(resultFuture.isDone());
        assertTrue(resultFuture.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, resultFuture::get);
        assertInstanceOf(ShareSessionNotFoundException.class, exception.getCause());
    }

    @Test
    public void testReleaseSessionWithEmptyTopicPartitions() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(shareSession);
        when(shareSession.partitionMap()).thenReturn(new ImplicitLinkedHashCollection<>());

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        // Empty list of TopicIdPartitions to releaseSession. This should return an empty map.
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseSession(groupId, memberId.toString());
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testReleaseSessionWithNullShareSession() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        ShareSessionCache cache = mock(ShareSessionCache.class);
        // Null share session in get response so empty topic partitions should be returned.
        when(cache.get(new ShareSessionKey(groupId, memberId))).thenReturn(null);
        // Make the response not null for remove so can further check for the return value from topic partitions.
        when(cache.remove(new ShareSessionKey(groupId, memberId))).thenReturn(mock(ShareSession.class));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withCache(cache)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
            sharePartitionManager.releaseSession(groupId, memberId.toString());
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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp), sp);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, Arrays.asList(
            new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp).errorCode());
    }

    @Test
    public void testAcknowledgeMultiplePartition() {
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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp3), sp3);

        Metrics metrics = new Metrics();
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withMetrics(metrics).build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp1, Arrays.asList(
                new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp2, Arrays.asList(
                new ShareAcknowledgementBatch(15, 26, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(34, 56, Collections.singletonList((byte) 2))
        ));
        acknowledgeTopics.put(tp3, Arrays.asList(
                new ShareAcknowledgementBatch(4, 15, Collections.singletonList((byte) 3)),
                new ShareAcknowledgementBatch(16, 21, Collections.singletonList((byte) 3))
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

        Map<MetricName, Consumer<Double>> expectedMetrics = new HashMap<>();
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.SHARE_ACK_COUNT, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME),
                val -> assertEquals(val, 1.0)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.SHARE_ACK_RATE, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME),
                val -> assertTrue(val > 0)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_COUNT, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.ACCEPT.toString())),
                val -> assertEquals(2.0, val)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_COUNT, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.RELEASE.toString())),
                val -> assertEquals(2.0, val)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_COUNT, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.REJECT.toString())),
                val -> assertEquals(2.0, val)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_RATE, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.ACCEPT.toString())),
                val -> assertTrue(val > 0)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_RATE, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.RELEASE.toString())),
                val -> assertTrue(val > 0)
        );
        expectedMetrics.put(
                metrics.metricName(SharePartitionManager.ShareGroupMetrics.RECORD_ACK_RATE, SharePartitionManager.ShareGroupMetrics.METRICS_GROUP_NAME,
                        Collections.singletonMap(SharePartitionManager.ShareGroupMetrics.ACK_TYPE, AcknowledgeType.REJECT.toString())),
                val -> assertTrue(val > 0)
        );
        expectedMetrics.forEach((metric, test) -> {
            assertTrue(metrics.metrics().containsKey(metric));
            test.accept((Double) metrics.metrics().get(metric).metricValue());
        });
    }

    @Test
    public void testAcknowledgeIncorrectGroupId() {
        String groupId = "grp";
        String groupId2 = "grp2";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp = mock(SharePartition.class);

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp), sp);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, Arrays.asList(
            new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId2, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp).errorCode());
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
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp), sp);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, Arrays.asList(
            new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
            new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
            sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(0, result.get(tp).partitionIndex());
        assertEquals(Errors.INVALID_REQUEST.code(), result.get(tp).errorCode());
    }

    @Test
    public void testAcknowledgeEmptyPartitionCacheMap() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo4", 3));
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp, Arrays.asList(
                new ShareAcknowledgementBatch(78, 90, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(94, 99, Collections.singletonList((byte) 2))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(3, result.get(tp).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp).errorCode());
    }

    @Test
    public void testAcknowledgeCompletesDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);

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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);

        ShareFetch shareFetch = new ShareFetch(
                FETCH_PARAMS,
                groupId,
                Uuid.randomUuid().toString(),
                new CompletableFuture<>(),
                partitionMaxBytes,
                100);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 2);

        // Initially you cannot acquire records for both sp1 and sp2.
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        partitionMaxBytes.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(mockReplicaManager)
            .withSharePartitions(sharePartitions)
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        doAnswer(invocation -> buildLogReadResult(partitionMaxBytes.keySet())).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics = new HashMap<>();
        acknowledgeTopics.put(tp1, Arrays.asList(
                new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
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
    }

    @Test
    public void testAcknowledgeDoesNotCompleteDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);

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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareFetch shareFetch = new ShareFetch(
                FETCH_PARAMS,
                groupId,
                Uuid.randomUuid().toString(),
                new CompletableFuture<>(),
                partitionMaxBytes,
                100);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Initially you cannot acquire records for both all 3 share partitions.
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp3.maybeAcquireFetchLock()).thenReturn(true);
        when(sp3.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        partitionMaxBytes.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
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
        acknowledgeTopics.put(tp3, Arrays.asList(
                new ShareAcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));

        // Acknowledgement request for sp3.
        sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);

        // Since neither sp1 and sp2 have been acknowledged, the delayedShareFetchPurgatory should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        Mockito.verify(sp1, times(0)).nextFetchOffset();
        Mockito.verify(sp2, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testReleaseSessionCompletesDelayedShareFetchRequest() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 0));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 0));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.remove(new ShareSessionKey(groupId, Uuid.fromString(memberId)))).thenReturn(shareSession);

        // mocked share partitions sp1 and sp2 can be acquired once there is a release acquired records on session close request for it.
        doAnswer(invocation -> {
            when(sp1.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp1).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));
        doAnswer(invocation -> {
            when(sp2.canAcquireRecords()).thenReturn(true);
            return CompletableFuture.completedFuture(Optional.empty());
        }).when(sp2).releaseAcquiredRecords(ArgumentMatchers.eq(memberId));

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);

        ShareFetch shareFetch = new ShareFetch(
                FETCH_PARAMS,
                groupId,
                Uuid.randomUuid().toString(),
                new CompletableFuture<>(),
                partitionMaxBytes,
                100);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(mockReplicaManager, tp1, 1);

        // Initially you cannot acquire records for both sp1 and sp2.
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        partitionMaxBytes.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        SharePartitionManager sharePartitionManager = spy(SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
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
            .build();

        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchWatchKeys);

        // Since acquisition lock for sp1 and sp2 cannot be acquired, we should have 2 watched keys.
        assertEquals(2, delayedShareFetchPurgatory.watched());

        doAnswer(invocation -> buildLogReadResult(partitionMaxBytes.keySet())).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        assertEquals(2, delayedShareFetchPurgatory.watched());

        // The share session for this share group member returns tp1 and tp3, tp1 is common in both the delayed fetch request and the share session.
        when(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, Uuid.fromString(memberId))).thenReturn(Arrays.asList(tp1, tp3));

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

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);

        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);

        ShareSessionCache cache = mock(ShareSessionCache.class);
        ShareSession shareSession = mock(ShareSession.class);
        when(cache.remove(new ShareSessionKey(groupId, Uuid.fromString(memberId)))).thenReturn(shareSession);

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

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp3), sp3);

        ShareFetch shareFetch = new ShareFetch(
                FETCH_PARAMS,
                groupId,
                Uuid.randomUuid().toString(),
                new CompletableFuture<>(),
                partitionMaxBytes,
                100);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Initially you cannot acquire records for both all 3 share partitions.
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp3.maybeAcquireFetchLock()).thenReturn(true);
        when(sp3.canAcquireRecords()).thenReturn(false);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        partitionMaxBytes.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        SharePartitionManager sharePartitionManager = spy(SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
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
        when(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, Uuid.fromString(memberId))).thenReturn(Collections.singletonList(tp3));

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
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);

        // Keep the initialization future pending, so fetch request is stuck.
        CompletableFuture<Void> pendingInitializationFuture = new CompletableFuture<>();
        when(sp0.maybeInitialize()).thenReturn(pendingInitializationFuture);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(mockReplicaManager).withTimer(mockTimer)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
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
        // Complete the pending initialization future.
        pendingInitializationFuture.complete(null);
    }

    @Test
    public void testDelayedInitializationShouldCompleteFetchRequest() throws Exception {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);

        // Keep the 2 initialization futures pending and 1 completed with leader not available exception.
        CompletableFuture<Void> pendingInitializationFuture1 = new CompletableFuture<>();
        CompletableFuture<Void> pendingInitializationFuture2 = new CompletableFuture<>();
        when(sp0.maybeInitialize()).
            thenReturn(pendingInitializationFuture1)
            .thenReturn(pendingInitializationFuture2)
            .thenReturn(CompletableFuture.failedFuture(new LeaderNotAvailableException("Leader not available")));

        DelayedOperationPurgatory<DelayedShareFetch> shareFetchPurgatorySpy = spy(new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true));
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, shareFetchPurgatorySpy);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(mockReplicaManager).withTimer(mockTimer)
            .build();

        // Send 3 requests for share fetch for same share partition.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future1 =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future2 =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future3 =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);

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
    }

    @Test
    public void testSharePartitionInitializationExceptions() throws Exception {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
                "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
                DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(mockReplicaManager).withTimer(mockTimer)
            .build();

        // Return LeaderNotAvailableException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new LeaderNotAvailableException("Leader not available")));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
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
        assertEquals(1, partitionCacheMap.size());

        // Return IllegalStateException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new IllegalStateException("Illegal state")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Illegal state");
        Mockito.verify(sp0, times(1)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return CoordinatorNotAvailableException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new CoordinatorNotAvailableException("Coordinator not available")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.COORDINATOR_NOT_AVAILABLE, "Coordinator not available");
        Mockito.verify(sp0, times(2)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return InvalidRequestException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new InvalidRequestException("Invalid request")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.INVALID_REQUEST, "Invalid request");
        Mockito.verify(sp0, times(3)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return FencedStateEpochException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new FencedStateEpochException("Fenced state epoch")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.FENCED_STATE_EPOCH, "Fenced state epoch");
        Mockito.verify(sp0, times(4)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return NotLeaderOrFollowerException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new NotLeaderOrFollowerException("Not leader or follower")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER, "Not leader or follower");
        Mockito.verify(sp0, times(5)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());

        // The last exception removes the share partition from the cache hence re-add the share partition to cache.
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Return RuntimeException to simulate initialization failure.
        when(sp0.maybeInitialize()).thenReturn(FutureUtils.failedFuture(new RuntimeException("Runtime exception")));
        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing in delayed share fetch queue never ended.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Runtime exception");
        Mockito.verify(sp0, times(6)).markFenced();
        assertTrue(partitionCacheMap.isEmpty());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testShareFetchProcessingExceptions() throws Exception {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        Map<SharePartitionKey, SharePartition> partitionCacheMap = (Map<SharePartitionKey, SharePartition>) mock(Map.class);
        // Throw the exception for first fetch request. Return share partition for next.
        when(partitionCacheMap.computeIfAbsent(any(), any()))
            .thenThrow(new RuntimeException("Error creating instance"));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Error creating instance");
    }

    @Test
    public void testSharePartitionInitializationFailure() throws Exception {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        // Send map to check no share partition is created.
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        // Validate when partition is not the leader.
        Partition partition = mock(Partition.class);
        when(partition.isLeader()).thenReturn(false);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        // First check should throw KafkaStorageException, second check should return partition which
        // is not leader.
        when(replicaManager.getPartitionOrException(any()))
            .thenThrow(new KafkaStorageException("Exception"))
            .thenReturn(partition);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withPartitionCacheMap(partitionCacheMap)
            .build();

        // Validate when exception is thrown.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.KAFKA_STORAGE_ERROR, "Exception");
        assertTrue(partitionCacheMap.isEmpty());

        // Validate when partition is not leader.
        future = sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, partitionMaxBytes);
        TestUtils.waitForCondition(
            future::isDone,
            DELAYED_SHARE_FETCH_TIMEOUT_MS,
            () -> "Processing for delayed share fetch request not finished.");
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER);
        assertTrue(partitionCacheMap.isEmpty());
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
        Map<TopicIdPartition, Integer> partitionMaxBytes = Map.of(
            tp0, PARTITION_MAX_BYTES,
            tp1, PARTITION_MAX_BYTES,
            tp2, PARTITION_MAX_BYTES);

        // Mark partition0 as not the leader.
        Partition partition0 = mock(Partition.class);
        when(partition0.isLeader()).thenReturn(false);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getPartitionOrException(any()))
            .thenReturn(partition0);

        // Mock share partition for tp1, so it can succeed.
        SharePartition sp1 = mock(SharePartition.class);
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);

        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp1.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        when(sp1.acquire(anyString(), anyInt(), any())).thenReturn(new ShareAcquiredRecords(Collections.emptyList(), 0));

        // Fail initialization for tp2.
        SharePartition sp2 = mock(SharePartition.class);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp2), sp2);
        when(sp2.maybeInitialize()).thenReturn(CompletableFuture.failedFuture(new FencedStateEpochException("Fenced state epoch")));

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(replicaManager, delayedShareFetchPurgatory);
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp1, 1);

        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withPartitionCacheMap(partitionCacheMap)
            .build();

        // Validate when exception is thrown.
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, Uuid.randomUuid().toString(), FETCH_PARAMS, partitionMaxBytes);
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

        Mockito.verify(replicaManager, times(0)).completeDelayedShareFetchRequest(any());
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
    }

    @Test
    public void testReplicaManagerFetchException() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));
        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        doThrow(new RuntimeException("Exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        validateShareFetchFutureException(future, tp0, Errors.UNKNOWN_SERVER_ERROR, "Exception");
        // Verify that the share partition is still in the cache on exception.
        assertEquals(1, partitionCacheMap.size());

        // Throw NotLeaderOrFollowerException from replica manager fetch which should evict instance from the cache.
        doThrow(new NotLeaderOrFollowerException("Leader exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        validateShareFetchFutureException(future, tp0, Errors.NOT_LEADER_OR_FOLLOWER, "Leader exception");
        assertTrue(partitionCacheMap.isEmpty());
    }

    @Test
    public void testReplicaManagerFetchMultipleSharePartitionsException() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));

        SharePartition sp1 = mock(SharePartition.class);
        // Do not make the share partition acquirable hence it shouldn't be removed from the cache,
        // as it won't be part of replica manger readFromLog request.
        when(sp1.maybeAcquireFetchLock()).thenReturn(false);
        when(sp1.maybeInitialize()).thenReturn(CompletableFuture.completedFuture(null));

        Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp1), sp1);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, mockReplicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(mockReplicaManager, delayedShareFetchPurgatory);

        // Throw FencedStateEpochException from replica manager fetch which should evict instance from the cache.
        doThrow(new FencedStateEpochException("Fenced exception")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap)
            .withReplicaManager(mockReplicaManager)
            .withTimer(mockTimer)
            .build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        validateShareFetchFutureException(future, tp0, Errors.FENCED_STATE_EPOCH, "Fenced exception");
        // Verify that tp1 is still in the cache on exception.
        assertEquals(1, partitionCacheMap.size());
        assertEquals(sp1, partitionCacheMap.get(new SharePartitionKey(groupId, tp1)));

        // Make sp1 acquirable and add sp0 back in partition cache. Both share partitions should be
        // removed from the cache.
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        partitionCacheMap.put(new SharePartitionKey(groupId, tp0), sp0);
        // Throw FencedStateEpochException from replica manager fetch which should evict instance from the cache.
        doThrow(new FencedStateEpochException("Fenced exception again")).when(mockReplicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        future = sharePartitionManager.fetchMessages(groupId, memberId.toString(), FETCH_PARAMS, partitionMaxBytes);
        validateShareFetchFutureException(future, List.of(tp0, tp1), Errors.FENCED_STATE_EPOCH, "Fenced exception again");
        assertTrue(partitionCacheMap.isEmpty());
    }

    private ShareFetchResponseData.PartitionData noErrorShareFetchResponse() {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0);
    }

    private ShareFetchResponseData.PartitionData errorShareFetchResponse(Short errorCode) {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0).setErrorCode(errorCode);
    }

    private void mockUpdateAndGenerateResponseData(ShareFetchContext context, String groupId, Uuid memberId) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> data = new LinkedHashMap<>();
        if (context.getClass() == ShareSessionContext.class) {
            ShareSessionContext shareSessionContext = (ShareSessionContext) context;
            if (!shareSessionContext.isSubsequent()) {
                shareSessionContext.shareFetchData().forEach((topicIdPartition, sharePartitionData) -> data.put(topicIdPartition,
                        topicIdPartition.topic() == null ? errorShareFetchResponse(Errors.UNKNOWN_TOPIC_ID.code()) :
                                noErrorShareFetchResponse()));
            } else {
                synchronized (shareSessionContext.session()) {
                    shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                                TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
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
            context.shareFetchData().forEach((topicIdPartition, sharePartitionData) ->
                    partitionsInContext.add(topicIdPartition));
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
        Set<TopicIdPartition> actualValidPartitions = new HashSet<>();
        erroneousAndValidPartitionData.erroneous().forEach((topicIdPartition, partitionData) ->
                actualErroneousPartitions.add(topicIdPartition));
        erroneousAndValidPartitionData.validTopicIdPartitions().forEach((topicIdPartition, partitionData) ->
                actualValidPartitions.add(topicIdPartition));
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
        validateShareFetchFutureException(future, Collections.singletonList(topicIdPartition), error, null);
    }

    private void validateShareFetchFutureException(CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
        TopicIdPartition topicIdPartition, Errors error, String message) {
        validateShareFetchFutureException(future, Collections.singletonList(topicIdPartition), error, message);
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
        Mockito.doReturn(new OffsetResultHolder(Option.apply(timestampAndOffset), Option.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    static Seq<Tuple2<TopicIdPartition, LogReadResult>> buildLogReadResult(Set<TopicIdPartition> topicIdPartitions) {
        List<Tuple2<TopicIdPartition, LogReadResult>> logReadResults = new ArrayList<>();
        topicIdPartitions.forEach(topicIdPartition -> logReadResults.add(new Tuple2<>(topicIdPartition, new LogReadResult(
            new FetchDataInfo(new LogOffsetMetadata(0, 0, 0), MemoryRecords.EMPTY),
            Option.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            Option.empty(),
            Option.empty(),
            Option.empty()
        ))));
        return CollectionConverters.asScala(logReadResults).toSeq();
    }

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

    static class SharePartitionManagerBuilder {
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private Time time = new MockTime();
        private ShareSessionCache cache = new ShareSessionCache(10, 1000);
        private Map<SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        private Persister persister = new NoOpShareStatePersister();
        private Timer timer = new MockTimer();
        private Metrics metrics = new Metrics();

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

        SharePartitionManagerBuilder withPartitionCacheMap(Map<SharePartitionKey, SharePartition> partitionCacheMap) {
            this.partitionCacheMap = partitionCacheMap;
            return this;
        }

        private SharePartitionManagerBuilder withShareGroupPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        private SharePartitionManagerBuilder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        private SharePartitionManagerBuilder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public static SharePartitionManagerBuilder builder() {
            return new SharePartitionManagerBuilder();
        }

        public SharePartitionManager build() {
            return new SharePartitionManager(replicaManager,
                    time,
                    cache,
                    partitionCacheMap,
                    DEFAULT_RECORD_LOCK_DURATION_MS,
                    timer,
                    MAX_DELIVERY_COUNT,
                    MAX_IN_FLIGHT_MESSAGES,
                    MAX_FETCH_RECORDS,
                    persister,
                    mock(GroupConfigManager.class),
                    metrics);
        }
    }
}
