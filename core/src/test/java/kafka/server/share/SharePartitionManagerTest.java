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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidShareSessionEpochException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.NoOpShareStatePersister;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ErroneousAndValidPartitionData;
import org.apache.kafka.server.share.FinalContext;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.ShareFetchContext;
import org.apache.kafka.server.share.ShareSession;
import org.apache.kafka.server.share.ShareSessionCache;
import org.apache.kafka.server.share.ShareSessionContext;
import org.apache.kafka.server.share.ShareSessionKey;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;

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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import scala.Tuple2;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(120)
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class SharePartitionManagerTest {

    private static final int RECORD_LOCK_DURATION_MS = 30000;
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    private static final int PARTITION_MAX_BYTES = 40000;

    private static Timer mockTimer;

    private static final List<TopicIdPartition> EMPTY_PART_LIST = Collections.unmodifiableList(new ArrayList<>());

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("sharePartitionManagerTestReaper",
            new SystemTimer("sharePartitionManagerTestTimer"));
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

        // shareFetchData is not empty, but the maxBytes of topic partition is 0, which means this is added only for acknowledgements.
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

        // shareFetchData is not empty and the maxBytes of topic partition is not 0, which means this is trying to fetch on a Final request.
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
        SharePartitionManager.SharePartitionKey sharePartitionKey1 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey2 = new SharePartitionManager.SharePartitionKey("mock-group-2",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey3 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(1L, 1L), new TopicPartition("test-1", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey4 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 1)));
        SharePartitionManager.SharePartitionKey sharePartitionKey5 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 0L), new TopicPartition("test-2", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey1Copy = new SharePartitionManager.SharePartitionKey("mock-group-1",
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
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
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

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(0L).thenReturn(100L);
        Metrics metrics = new Metrics();
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withReplicaManager(replicaManager)
            .withTime(time)
            .withMetrics(metrics)
            .build();

        doAnswer(invocation -> {
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).when(replicaManager).fetchMessages(any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, partitionMaxBytes);
        Mockito.verify(replicaManager, times(1)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, partitionMaxBytes);
        Mockito.verify(replicaManager, times(2)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, partitionMaxBytes);
        Mockito.verify(replicaManager, times(3)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());

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
    public void testProcessFetchResponse() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
            k -> new SharePartition(groupId, tp0, MAX_IN_FLIGHT_MESSAGES, MAX_DELIVERY_COUNT,
                RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), NoOpShareStatePersister.getInstance()));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
            k -> new SharePartition(groupId, tp1, MAX_IN_FLIGHT_MESSAGES, MAX_DELIVERY_COUNT,
                RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), NoOpShareStatePersister.getInstance()));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
                future, partitionMaxBytes);

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        MemoryRecords records1 = MemoryRecords.withRecords(100L, Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 100L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)),
            resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
            resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testProcessFetchResponseWithEmptyRecords() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
            k -> new SharePartition(groupId, tp0, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), NoOpShareStatePersister.getInstance()));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
            k -> new SharePartition(groupId, tp1, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), NoOpShareStatePersister.getInstance()));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
            future, partitionMaxBytes);

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.emptyList(), resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.emptyList(), resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testMultipleConcurrentShareFetches() throws InterruptedException {

        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
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
        ReplicaManager replicaManager = mock(ReplicaManager.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
            k -> new SharePartition(groupId, tp0, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, time, NoOpShareStatePersister.getInstance()));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
            k -> new SharePartition(groupId, tp1, MAX_DELIVERY_COUNT,  MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, time, NoOpShareStatePersister.getInstance()));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp2),
            k -> new SharePartition(groupId, tp2, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, time, NoOpShareStatePersister.getInstance()));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp3),
            k -> new SharePartition(groupId, tp3, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS, mockTimer, time, NoOpShareStatePersister.getInstance()));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withTime(time).withReplicaManager(replicaManager).build();

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
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(15, sp0.nextFetchOffset());
            assertEquals(1, sp1.nextFetchOffset());
            assertEquals(25, sp2.nextFetchOffset());
            assertEquals(15, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(6, sp0.nextFetchOffset());
            assertEquals(18, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(23, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(30, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(25, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).when(replicaManager).fetchMessages(any(), any(), any(ReplicaQuota.class), any());

        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i != threadCount; ++i) {
                executorService.submit(() -> {
                    sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, partitionMaxBytes);
                });
                // We are blocking the main thread at an interval of 10 threads so that the currently running executorService threads can complete.
                if (i % 10 == 0)
                    executorService.awaitTermination(50, TimeUnit.MILLISECONDS);
            }
        } finally {
            if (!executorService.awaitTermination(50, TimeUnit.MILLISECONDS))
                executorService.shutdown();
        }
        // We are checking the number of replicaManager fetchMessages() calls
        Mockito.verify(replicaManager, atMost(100)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());
        Mockito.verify(replicaManager, atLeast(10)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());
    }

    @Test
    public void testReplicaManagerFetchShouldNotProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(false);
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), fetchParams, partitionMaxBytes);
        Mockito.verify(replicaManager, times(0)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = future.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testReplicaManagerFetchShouldProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        sharePartitionManager.fetchMessages(groupId, memberId.toString(), fetchParams, partitionMaxBytes);
        // Since the nextFetchOffset does not point to endOffset + 1, i.e. some of the records in the cachedState are AVAILABLE,
        // even though the maxInFlightMessages limit is exceeded, replicaManager.fetchMessages should be called
        Mockito.verify(replicaManager, times(1)).fetchMessages(
            any(), any(), any(ReplicaQuota.class), any());
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
        Mockito.verify(persister, times(1)).stop();
    }

    @Test
    public void testCloseShouldCompletePendingFetchRequests() throws Exception {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();

        // Acquire the fetch lock so fetch requests keep waiting in the queue.
        assertTrue(sharePartitionManager.acquireProcessFetchQueueLock());
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
            sharePartitionManager.fetchMessages(groupId, memberId.toString(), fetchParams, partitionMaxBytes);
        // Verify that the fetch request is not completed.
        assertFalse(future.isDone());

        // Closing the sharePartitionManager closes pending fetch requests in the fetch queue.
        sharePartitionManager.close();
        // Verify that the fetch request is now completed.
        assertTrue(future.isDone());
        assertFutureThrows(future, BrokerNotAvailableException.class);
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);

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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp), sp);
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp3), sp3);

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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp), sp);
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
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp), sp);
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
    public void testProcessFetchResponseWithLsoMovementForTopicPartition() {
        String groupId = "grp";
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        SharePartitionManager sharePartitionManagersSpy = Mockito.spy(sharePartitionManager);

        // Mocking the offsetForEarliestTimestamp method to return a valid LSO.
        Mockito.doReturn(1L).when(sharePartitionManagersSpy).offsetForEarliestTimestamp(any(TopicIdPartition.class));

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.emptyList()),
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1))));
        when(sp1.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1))),
                CompletableFuture.completedFuture(Collections.emptyList()));

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
                groupId, Uuid.randomUuid().toString(), future, partitionMaxBytes);

        MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData1 = new ArrayList<>();
        responseData1.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData1.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result1 =
                sharePartitionManagersSpy.processFetchResponse(shareFetchPartitionData, responseData1);

        assertTrue(result1.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData1 = result1.join();
        assertEquals(2, resultData1.size());
        assertTrue(resultData1.containsKey(tp0));
        assertTrue(resultData1.containsKey(tp1));
        assertEquals(0, resultData1.get(tp0).partitionIndex());
        assertEquals(1, resultData1.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData1.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData1.get(tp1).errorCode());

        // Since we have OFFSET_OUT_OF_RANGE exception for tp1 and no exception for tp2 from SharePartition class,
        // we should have 1 call for updateCacheAndOffsets for tp0 and 0 calls for tp1.
        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(any(Long.class));
        Mockito.verify(sp1, times(0)).updateCacheAndOffsets(any(Long.class));

        MemoryRecords records2 = MemoryRecords.withRecords(100L, Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData2 = new ArrayList<>();
        responseData2.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData2.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result2 =
                sharePartitionManagersSpy.processFetchResponse(shareFetchPartitionData, responseData2);

        assertTrue(result2.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData2 = result2.join();
        assertEquals(2, resultData2.size());
        assertTrue(resultData2.containsKey(tp0));
        assertTrue(resultData2.containsKey(tp1));
        assertEquals(0, resultData2.get(tp0).partitionIndex());
        assertEquals(1, resultData2.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData2.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData2.get(tp1).errorCode());

        // Since we don't see any exception for tp1 and tp2 from SharePartition class,
        // the updateCacheAndOffsets calls should remain the same as the previous case.
        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(any(Long.class));
        Mockito.verify(sp1, times(0)).updateCacheAndOffsets(any(Long.class));
    }

    @Test
    public void testFetchQueueProcessingWhenFrontItemIsEmpty() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        final Time time = new MockTime();
        ReplicaManager replicaManager = mock(ReplicaManager.class);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData1 = new SharePartitionManager.ShareFetchPartitionData(
                fetchParams, groupId, memberId, new CompletableFuture<>(), partitionMaxBytes);
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData2 = new SharePartitionManager.ShareFetchPartitionData(
            fetchParams, groupId, memberId, new CompletableFuture<>(), partitionMaxBytes);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
                key -> new SharePartition(groupId, tp0, MAX_IN_FLIGHT_MESSAGES, MAX_DELIVERY_COUNT,
                        RECORD_LOCK_DURATION_MS, mockTimer, time, NoOpShareStatePersister.getInstance()));

        ConcurrentLinkedQueue<SharePartitionManager.ShareFetchPartitionData> fetchQueue = new ConcurrentLinkedQueue<>();
        // First request added to fetch queue is empty i.e. no topic partitions to fetch.
        fetchQueue.add(shareFetchPartitionData1);
        // Second request added to fetch queue has a topic partition to fetch.
        fetchQueue.add(shareFetchPartitionData2);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
            .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).withTime(time)
            .withFetchQueue(fetchQueue).build();
        sharePartitionManager.maybeProcessFetchQueue();

        // Verifying that the second item in the fetchQueue is processed, even though the first item is empty.
        verify(replicaManager, times(1)).fetchMessages(any(), any(), any(ReplicaQuota.class), any());
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

    private static class SharePartitionManagerBuilder {
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private Time time = new MockTime();
        private ShareSessionCache cache = new ShareSessionCache(10, 1000);
        private Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        private Persister persister = NoOpShareStatePersister.getInstance();
        private Timer timer = new MockTimer();
        private Metrics metrics = new Metrics();
        private ConcurrentLinkedQueue<SharePartitionManager.ShareFetchPartitionData> fetchQueue = new ConcurrentLinkedQueue<>();

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

        private SharePartitionManagerBuilder withPartitionCacheMap(Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap) {
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

        private SharePartitionManagerBuilder withFetchQueue(ConcurrentLinkedQueue<SharePartitionManager.ShareFetchPartitionData> fetchQueue) {
            this.fetchQueue = fetchQueue;
            return this;
        }

        public static SharePartitionManagerBuilder builder() {
            return new SharePartitionManagerBuilder();
        }

        public SharePartitionManager build() {
            return new SharePartitionManager(replicaManager, time, cache, partitionCacheMap, fetchQueue, RECORD_LOCK_DURATION_MS, timer, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES, persister, metrics);
        }
    }
}
