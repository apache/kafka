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
package org.apache.kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.server.FetchContext.FullFetchContext;
import org.apache.kafka.server.FetchContext.IncrementalFetchContext;
import org.apache.kafka.server.FetchContext.SessionErrorContext;
import org.apache.kafka.server.FetchContext.SessionlessFetchContext;
import org.apache.kafka.server.FetchSession.CachedPartition;
import org.apache.kafka.server.FetchSession.FetchSessionCache;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.requests.FetchMetadata.FINAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INITIAL;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class FetchSessionTest {
    @AfterEach
    public void afterEach() {
        FetchSessionCache.METRICS_GROUP.removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_SESSIONS);
        FetchSessionCache.METRICS_GROUP.removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED);
        FetchSessionCache.METRICS_GROUP.removeMetric(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC);
        FetchSessionCache.COUNTER.set(0);
    }

    @Test
    public void testNewSessionId() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(3, 100, Integer.MAX_VALUE, 0);
        for (int i = 0; i < 10_000; i++) {
            int id = cacheShard.newSessionId();
            assertTrue(id > 0);
        }
    }

    @Test
    public void testSessionCache() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(3, 100, Integer.MAX_VALUE, 0);
        assertEquals(0, cacheShard.size());

        int id1 = cacheShard.maybeCreateSession(0, false, 10, true, () -> createPartitions(10));
        int id2 = cacheShard.maybeCreateSession(10, false, 20, true, () -> createPartitions(20));
        int id3 = cacheShard.maybeCreateSession(20, false, 30, true, () -> createPartitions(30));
        assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(30, false, 40, true, () -> createPartitions(40)));
        assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(40, false, 5, true, () -> createPartitions(5)));
        assertCacheContains(cacheShard, id1, id2, id3);

        cacheShard.touch(cacheShard.get(id1).orElseThrow(), 200);
        int id4 = cacheShard.maybeCreateSession(210, false, 11, true, () -> createPartitions(11));
        assertCacheContains(cacheShard, id1, id3, id4);

        cacheShard.touch(cacheShard.get(id1).orElseThrow(), 400);
        cacheShard.touch(cacheShard.get(id3).orElseThrow(), 390);
        cacheShard.touch(cacheShard.get(id4).orElseThrow(), 400);
        int id5 = cacheShard.maybeCreateSession(410, false, 50, true, () -> createPartitions(50));
        assertCacheContains(cacheShard, id3, id4, id5);
        assertEquals(INVALID_SESSION_ID, cacheShard.maybeCreateSession(410, false, 5, true, () -> createPartitions(5)));

        int id6 = cacheShard.maybeCreateSession(410, true, 5, true, () -> createPartitions(5));
        assertCacheContains(cacheShard, id3, id5, id6);
    }

    @Test
    public void testResizeCachedSessions() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(2, 100, Integer.MAX_VALUE, 0);
        assertEquals(0, cacheShard.totalPartitions());
        assertEquals(0, cacheShard.size());
        assertEquals(0, cacheShard.evictionsMeter().count());

        int id1 = cacheShard.maybeCreateSession(0, false, 2, true, () -> createPartitions(2));
        assertTrue(id1 > 0);
        assertCacheContains(cacheShard, id1);

        FetchSession session1 = cacheShard.get(id1).orElseThrow();
        assertEquals(2, session1.size());
        assertEquals(2, cacheShard.totalPartitions());
        assertEquals(1, cacheShard.size());
        assertEquals(0, cacheShard.evictionsMeter().count());

        int id2 = cacheShard.maybeCreateSession(0, false, 4, true, () -> createPartitions(4));
        FetchSession session2 = cacheShard.get(id2).orElseThrow();
        assertTrue(id2 > 0);
        assertCacheContains(cacheShard, id1, id2);
        assertEquals(6, cacheShard.totalPartitions());
        assertEquals(2, cacheShard.size());
        assertEquals(0, cacheShard.evictionsMeter().count());

        cacheShard.touch(session1, 200);
        cacheShard.touch(session2, 200);
        int id3 = cacheShard.maybeCreateSession(200, false, 5, true, () -> createPartitions(5));
        assertTrue(id3 > 0);
        assertCacheContains(cacheShard, id2, id3);
        assertEquals(9, cacheShard.totalPartitions());
        assertEquals(2, cacheShard.size());
        assertEquals(1, cacheShard.evictionsMeter().count());

        cacheShard.remove(id3);
        assertCacheContains(cacheShard, id2);
        assertEquals(1, cacheShard.size());
        assertEquals(1, cacheShard.evictionsMeter().count());
        assertEquals(4, cacheShard.totalPartitions());

        Iterator<CachedPartition> iter = session2.partitionMap().iterator();
        iter.next();
        iter.remove();
        assertEquals(3, session2.size());
        assertEquals(4, session2.cachedSize());

        cacheShard.touch(session2, session2.lastUsedMs());
        assertEquals(3, cacheShard.totalPartitions());
    }

    @Test
    public void testCachedLeaderEpoch() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);

        Map<String, Uuid> topicIds = Map.of("foo", Uuid.randomUuid(), "bar", Uuid.randomUuid());
        TopicIdPartition tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1));
        Map<Uuid, String> topicNames = topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> requestData1 = new LinkedHashMap<>();
        requestData1.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.empty()));
        requestData1.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.of(1)));
        requestData1.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 10, 0, 100, Optional.of(2)));

        FetchRequest request1 = createRequest(INITIAL, requestData1, List.of(), false, FETCH.latestVersion());
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.of(1), tp2, Optional.of(2)), cachedLeaderEpochs(context1));

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response = new LinkedHashMap<>();
        response.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp0.partition())
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        response.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition())
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        response.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp2.partition())
            .setHighWatermark(5)
            .setLastStableOffset(5)
            .setLogStartOffset(5));

        int sessionId = context1.updateAndGenerateResponseData(response, List.of()).sessionId();

        // With no changes, the cached epochs should remain the same
        FetchRequest request2 = createRequest(new FetchMetadata(sessionId, 1), new LinkedHashMap<>(),
            List.of(), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.of(1), tp2, Optional.of(2)), cachedLeaderEpochs(context2));
        context2.updateAndGenerateResponseData(response, List.of()).sessionId();

        // Now verify we can change the leader epoch and the context is updated
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> requestData3 = new LinkedHashMap<>();
        requestData3.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.of(6)));
        requestData3.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.empty()));
        requestData3.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 10, 0, 100, Optional.of(3)));

        FetchRequest request3 = createRequest(new FetchMetadata(sessionId, 2), requestData3,
            List.of(), false, FETCH.latestVersion());
        FetchContext context3 = newContext(fetchManager, request3, topicNames);
        assertEquals(Map.of(tp0, Optional.of(6), tp1, Optional.empty(), tp2, Optional.of(3)), cachedLeaderEpochs(context3));
    }

    @Test
    public void testLastFetchedEpoch() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);

        Map<String, Uuid> topicIds = Map.of("foo", Uuid.randomUuid(), "bar", Uuid.randomUuid());
        Map<Uuid, String> topicNames = topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1));

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> requestData1 = new LinkedHashMap<>();
        requestData1.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.empty(), Optional.empty()));
        requestData1.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.of(1), Optional.empty()));
        requestData1.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 10, 0, 100, Optional.of(2), Optional.of(1)));

        FetchRequest request1 = createRequest(INITIAL, requestData1, List.of(), false, FETCH.latestVersion());
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.of(1), tp2, Optional.of(2)), cachedLeaderEpochs(context1));
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.empty(), tp2, Optional.of(1)), cachedLastFetchedEpochs(context1));

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response = new LinkedHashMap<>();
        response.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp0.partition())
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        response.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition())
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        response.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp2.partition())
            .setHighWatermark(5)
            .setLastStableOffset(5)
            .setLogStartOffset(5));

        int sessionId = context1.updateAndGenerateResponseData(response, List.of()).sessionId();

        // With no changes, the cached epochs should remain the same
        FetchRequest request2 = createRequest(new FetchMetadata(sessionId, 1), new LinkedHashMap<>(),
            List.of(), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.of(1), tp2, Optional.of(2)), cachedLeaderEpochs(context2));
        assertEquals(Map.of(tp0, Optional.empty(), tp1, Optional.empty(), tp2, Optional.of(1)), cachedLastFetchedEpochs(context2));
        context2.updateAndGenerateResponseData(response, List.of()).sessionId();

        // Now verify we can change the leader epoch and the context is updated
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> requestData3 = new LinkedHashMap<>();
        requestData3.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.of(6), Optional.of(5)));
        requestData3.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.empty(), Optional.empty()));
        requestData3.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 10, 0, 100, Optional.of(3), Optional.of(3)));

        FetchRequest request3 = createRequest(new FetchMetadata(sessionId, 2), requestData3, List.of(), false, FETCH.latestVersion());
        FetchContext context3 = newContext(fetchManager, request3, topicNames);
        assertEquals(Map.of(tp0, Optional.of(6), tp1, Optional.empty(), tp2, Optional.of(3)), cachedLeaderEpochs(context3));
        assertEquals(Map.of(tp0, Optional.of(5), tp1, Optional.empty(), tp2, Optional.of(3)), cachedLastFetchedEpochs(context2));
    }

    @Test
    public void testFetchRequests() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = Map.of(Uuid.randomUuid(), "foo", Uuid.randomUuid(), "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1));

        // Verify that SESSIONLESS requests get a SessionlessFetchContext
        FetchRequest request = createRequest(FetchMetadata.LEGACY, new HashMap<>(), List.of(), true, FETCH.latestVersion());
        FetchContext context = newContext(fetchManager, request, topicNames);
        assertInstanceOf(SessionlessFetchContext.class, context);

        // Create a new fetch session with a FULL fetch request
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.empty()));
        reqData2.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.empty()));
        FetchRequest request2 = createRequest(INITIAL, reqData2, List.of(), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertInstanceOf(FullFetchContext.class, context2);

        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> reqData2Iter = reqData2.entrySet().iterator();
        context2.foreachPartition((topicIdPart, data) -> {
            Map.Entry<TopicPartition, FetchRequest.PartitionData> entry = reqData2Iter.next();
            assertEquals(entry.getKey(), topicIdPart.topicPartition());
            assertEquals(topicIds.get(entry.getKey().topic()), topicIdPart.topicId());
            assertEquals(entry.getValue(), data);
        });
        assertEquals(0, context2.getFetchOffset(tp0).orElseThrow());
        assertEquals(10, context2.getFetchOffset(tp1).orElseThrow());

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData2.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse resp2 = context2.updateAndGenerateResponseData(respData2, List.of());
        assertEquals(Errors.NONE, resp2.error());
        assertTrue(resp2.sessionId() != INVALID_SESSION_ID);
        assertEquals(
            respData2.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().topicPartition(), Map.Entry::getValue)),
            resp2.responseData(topicNames, request2.version())
        );

        // Test trying to create a new session with an invalid epoch
        FetchRequest request3 = createRequest(new FetchMetadata(resp2.sessionId(), 5), reqData2,
            List.of(), false, FETCH.latestVersion());
        FetchContext context3 = newContext(fetchManager, request3, topicNames);
        assertInstanceOf(SessionErrorContext.class, context3);
        assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH, context3.updateAndGenerateResponseData(respData2, List.of()).error());

        // Test trying to create a new session with a non-existent session id
        FetchRequest request4 = createRequest(new FetchMetadata(resp2.sessionId() + 1, 1), reqData2,
            List.of(), false, FETCH.latestVersion());
        FetchContext context4 = newContext(fetchManager, request4, topicNames);
        assertEquals(Errors.FETCH_SESSION_ID_NOT_FOUND, context4.updateAndGenerateResponseData(respData2, List.of()).error());

        // Continue the first fetch session we created.
        FetchRequest request5 = createRequest(new FetchMetadata(resp2.sessionId(), 1), new LinkedHashMap<>(),
            List.of(), false, FETCH.latestVersion());
        FetchContext context5 = newContext(fetchManager, request5, topicNames);
        assertInstanceOf(IncrementalFetchContext.class, context5);

        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> reqData5Iter = reqData2.entrySet().iterator();
        context5.foreachPartition((topicIdPart, data) -> {
            Map.Entry<TopicPartition, FetchRequest.PartitionData> entry = reqData5Iter.next();
            assertEquals(entry.getKey(), topicIdPart.topicPartition());
            assertEquals(topicIds.get(entry.getKey().topic()), topicIdPart.topicId());
            assertEquals(entry.getValue(), data);
        });
        assertEquals(10, context5.getFetchOffset(tp1).orElseThrow());

        FetchResponse resp5 = context5.updateAndGenerateResponseData(respData2, List.of());
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(resp2.sessionId(), resp5.sessionId());
        assertEquals(0, resp5.responseData(topicNames, request5.version()).size());

        // Test setting an invalid fetch session epoch.
        FetchRequest request6 = createRequest(new FetchMetadata(resp2.sessionId(), 5), reqData2,
            List.of(), false, FETCH.latestVersion());
        FetchContext context6 = newContext(fetchManager, request6, topicNames);
        assertInstanceOf(SessionErrorContext.class, context6);
        assertEquals(Errors.INVALID_FETCH_SESSION_EPOCH, context6.updateAndGenerateResponseData(respData2, List.of()).error());

        // Test generating a throttled response for the incremental fetch session
        FetchRequest request7 = createRequest(new FetchMetadata(resp2.sessionId(), 2), new LinkedHashMap<>(),
            List.of(), false, FETCH.latestVersion());
        FetchContext context7 = newContext(fetchManager, request7, topicNames);
        FetchResponse resp7 = context7.getThrottledResponse(100, List.of());
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(resp2.sessionId(), resp7.sessionId());
        assertEquals(100, resp7.throttleTimeMs());

        // Close the incremental fetch session.
        int prevSessionId = resp5.sessionId();
        int nextSessionId;
        do {
            LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData8 = new LinkedHashMap<>();
            reqData8.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 0, 0, 100, Optional.empty()));
            reqData8.put(tp3.topicPartition(), new PartitionData(tp3.topicId(), 10, 0, 100, Optional.empty()));
            FetchRequest request8 = createRequest(new FetchMetadata(prevSessionId, FINAL_EPOCH), reqData8,
                List.of(), false, FETCH.latestVersion());
            FetchContext context8 = newContext(fetchManager, request8, topicNames);
            assertInstanceOf(SessionlessFetchContext.class, context8);
            assertEquals(0, cacheShard.size());

            LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
            respData8.put(tp2, new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setHighWatermark(100)
                .setLastStableOffset(100)
                .setLogStartOffset(100));
            respData8.put(tp3, new FetchResponseData.PartitionData()
                .setPartitionIndex(1)
                .setHighWatermark(100)
                .setLastStableOffset(100)
                .setLogStartOffset(100));
            FetchResponse resp8 = context8.updateAndGenerateResponseData(respData8, List.of());
            assertEquals(Errors.NONE, resp8.error());

            nextSessionId = resp8.sessionId();
        } while (nextSessionId == prevSessionId);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIncrementalFetchSession(boolean usesTopicIds) {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = usesTopicIds
            ? Map.of(Uuid.randomUuid(), "foo", Uuid.randomUuid(), "bar")
            : Map.of();
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        short version = usesTopicIds ? FETCH.latestVersion() : (short) 12;
        Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
        Uuid barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID);
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        // Create a new fetch session with foo-0 and foo-1
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        reqData1.put(tp1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequest(INITIAL, reqData1, List.of(), false, version);
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData1.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, resp1.responseData(topicNames, request1.version()).size());

        // Create an incremental fetch request that removes foo-0 and adds bar-0
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp2.topicPartition(), new PartitionData(barId, 15, 0, 0, Optional.empty()));
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), reqData2, List.of(tp0), false, version);
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertInstanceOf(IncrementalFetchContext.class, context2);

        Set<TopicIdPartition> parts = new LinkedHashSet<>();
        parts.add(tp1);
        parts.add(tp2);
        Iterator<TopicIdPartition> reqData2Iter = parts.iterator();
        context2.foreachPartition((topicIdPart, data) -> assertEquals(reqData2Iter.next(), topicIdPart));
        assertEquals(Optional.empty(), context2.getFetchOffset(tp0));
        assertEquals(10, context2.getFetchOffset(tp1).orElseThrow());
        assertEquals(15, context2.getFetchOffset(tp2).orElseThrow());
        assertEquals(Optional.empty(), context2.getFetchOffset(new TopicIdPartition(barId, new TopicPartition("bar", 2))));

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        respData2.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse resp2 = context2.updateAndGenerateResponseData(respData2, List.of());
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(1, resp2.responseData(topicNames, request2.version()).size());
        assertTrue(resp2.sessionId() > 0);
    }

    // This test simulates a request without IDs sent to a broker with IDs.
    @Test
    public void testFetchSessionWithUnknownIdOldRequestVersion() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = Map.of(Uuid.randomUuid(), "foo", Uuid.randomUuid(), "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));

        // Create a new fetch session with foo-0 and foo-1
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.empty()));
        reqData1.put(tp1.topicPartition(), new PartitionData(Uuid.ZERO_UUID, 10, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequestWithoutTopicIds(INITIAL, reqData1);
        // Simulate unknown topic ID for foo.
        Map<Uuid, String> topicNamesOnlyBar = Map.of(topicIds.get("bar"), "bar");
        // We should not throw error since we have an older request version.
        FetchContext context1 = newContext(fetchManager, request1, topicNamesOnlyBar);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData1.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        // Since we are ignoring IDs, we should have no errors.
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, resp1.responseData(topicNames, request1.version()).size());
        resp1.responseData(topicNames, request1.version()).forEach((tp, resp) ->
            assertEquals(Errors.NONE.code(), resp.errorCode()));
    }

    @Test
    public void testFetchSessionWithUnknownId() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        Uuid zarId = Uuid.randomUuid();
        Map<Uuid, String> topicNames = Map.of(fooId, "foo", barId, "bar", zarId, "zar");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition zar0 = new TopicIdPartition(zarId, new TopicPartition("zar", 0));
        TopicIdPartition emptyFoo0 = new TopicIdPartition(fooId, new TopicPartition(null, 0));
        TopicIdPartition emptyFoo1 = new TopicIdPartition(fooId, new TopicPartition(null, 1));
        TopicIdPartition emptyZar0 = new TopicIdPartition(zarId, new TopicPartition(null, 0));

        // Create a new fetch session with foo-0 and foo-1
        LinkedHashMap<TopicPartition, PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0.topicPartition(), new PartitionData(foo0.topicId(), 0, 0, 100, Optional.empty()));
        reqData1.put(foo1.topicPartition(), new PartitionData(foo1.topicId(), 10, 0, 100, Optional.empty()));
        reqData1.put(zar0.topicPartition(), new PartitionData(zar0.topicId(), 10, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequest(INITIAL, reqData1, List.of(), false, FETCH.latestVersion());
        // Simulate unknown topic ID for foo.
        Map<Uuid, String> topicNamesOnlyBar = Map.of(barId, "bar");
        // We should not throw error since we have an older request version.
        FetchContext context1 = newContext(fetchManager, request1, topicNamesOnlyBar);
        assertInstanceOf(FullFetchContext.class, context1);
        assertPartitionsOrder(context1, List.of(emptyFoo0, emptyFoo1, emptyZar0));

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(emptyFoo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()));
        respData1.put(emptyFoo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()));
        respData1.put(emptyZar0, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        // On the latest request version, we should have unknown topic ID errors.
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);
        assertEquals(
            Map.of(
                foo0.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code(),
                foo1.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code(),
                zar0.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code()
            ),
            resp1.responseData(topicNames, request1.version()).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().errorCode()))
        );

        // Create an incremental request where we resolve the partitions
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), new LinkedHashMap<>(), List.of(), false, FETCH.latestVersion());
        Map<Uuid, String> topicNamesNoZar = Map.of(fooId, "foo", barId, "bar");
        FetchContext context2 = newContext(fetchManager, request2, topicNamesNoZar);
        assertInstanceOf(IncrementalFetchContext.class, context2);
        // Topic names in the session but not in the request are lazily resolved via foreachPartition. Resolve foo topic IDs here.
        assertPartitionsOrder(context2, List.of(foo0, foo1, emptyZar0));

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData2.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        respData2.put(emptyZar0, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()));
        FetchResponse resp2 = context2.updateAndGenerateResponseData(respData2, List.of());
        // Since we are ignoring IDs, we should have no errors.
        assertEquals(Errors.NONE, resp2.error());
        assertTrue(resp2.sessionId() != INVALID_SESSION_ID);
        assertEquals(3, resp2.responseData(topicNames, request2.version()).size());
        assertEquals(
            Map.of(
                foo0.topicPartition(), Errors.NONE.code(),
                foo1.topicPartition(), Errors.NONE.code(),
                zar0.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code()
            ),
            resp2.responseData(topicNames, request2.version()).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().errorCode()))
        );
    }

    @Test
    public void testIncrementalFetchSessionWithIdsWhenSessionDoesNotUseIds() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = new HashMap<>();
        TopicIdPartition foo0 = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0));

        // Create a new fetch session with foo-0
        LinkedHashMap<TopicPartition, PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0.topicPartition(), new PartitionData(Uuid.ZERO_UUID, 0, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequestWithoutTopicIds(INITIAL, reqData1);
        // Start a fetch session using a request version that does not use topic IDs.
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);

        // Create an incremental fetch request as though no topics changed. However, send a v13 request.
        // Also simulate the topic ID found on the server.
        topicNames.put(Uuid.randomUuid(), "foo");
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), new LinkedHashMap<>(), List.of(), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);

        assertInstanceOf(SessionErrorContext.class, context2);
        assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR, context2.updateAndGenerateResponseData(new LinkedHashMap<>(), List.of()).error());
    }

    @Test
    public void testIncrementalFetchSessionWithoutIdsWhenSessionUsesIds() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Uuid fooId = Uuid.randomUuid();
        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));

        // Create a new fetch session with foo-0
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequest(INITIAL, reqData1, List.of(), false, FETCH.latestVersion());
        // Start a fetch session using a request version that uses topic IDs.
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);

        // Create an incremental fetch request as though no topics changed. However, send a v12 request.
        // Also simulate the topic ID not found on the server
        topicNames.remove(fooId);

        FetchRequest request2 = createRequestWithoutTopicIds(new FetchMetadata(resp1.sessionId(), 1), new LinkedHashMap<>());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);

        assertInstanceOf(SessionErrorContext.class, context2);
        assertEquals(Errors.FETCH_SESSION_TOPIC_ID_ERROR, context2.updateAndGenerateResponseData(new LinkedHashMap<>(), List.of()).error());
    }

    // This test simulates a session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
    // -- as though the topic is deleted and recreated.
    @Test
    public void testFetchSessionUpdateTopicIdsBrokerSide() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = Map.of(Uuid.randomUuid(), "foo", Uuid.randomUuid(), "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 1));

        // Create a new fetch session with foo-0 and bar-1
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0.topicPartition(), new PartitionData(tp0.topicId(), 0, 0, 100, Optional.empty()));
        reqData1.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 10, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequest(INITIAL, reqData1, List.of(), false, FETCH.latestVersion());
        // Start a fetch session. Simulate unknown partition foo-0.
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        respData1.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(-1)
            .setLastStableOffset(-1)
            .setLogStartOffset(-1)
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, resp1.responseData(topicNames, request1.version()).size());

        // Create an incremental fetch request as though no topics changed.
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), new LinkedHashMap<>(), List.of(), false, FETCH.latestVersion());
        // Simulate ID changing on server.
        Map<Uuid, String> topicNamesFooChanged = Map.of(topicIds.get("bar"), "bar", Uuid.randomUuid(), "foo");
        FetchContext context2 = newContext(fetchManager, request2, topicNamesFooChanged);
        assertInstanceOf(IncrementalFetchContext.class, context2);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
        respData2.put(tp0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(-1)
            .setLastStableOffset(-1)
            .setLogStartOffset(-1)
            .setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code()));
        FetchResponse resp2 = context2.updateAndGenerateResponseData(respData2, List.of());
        assertEquals(Errors.NONE, resp2.error());
        assertTrue(resp2.sessionId() > 0);

        LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> responseData2 = resp2.responseData(topicNames, request2.version());
        // We should have the inconsistent topic ID error on the partition
        assertEquals(Errors.INCONSISTENT_TOPIC_ID.code(), responseData2.get(tp0.topicPartition()).errorCode());
    }

    @Test
    public void testResolveUnknownPartitions() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);

        TopicIdPartition foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));
        TopicIdPartition zar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("zar", 0));

        TopicIdPartition fooUnresolved = new TopicIdPartition(foo.topicId(), new TopicPartition(null, foo.partition()));
        TopicIdPartition barUnresolved = new TopicIdPartition(bar.topicId(), new TopicPartition(null, bar.partition()));
        TopicIdPartition zarUnresolved = new TopicIdPartition(zar.topicId(), new TopicPartition(null, zar.partition()));

        // The metadata cache does not know about the topic.
        FetchContext context1 = newContext(INITIAL, List.of(foo, bar, zar), fetchManager, Map.of());

        // So the context contains unresolved partitions.
        assertInstanceOf(FullFetchContext.class, context1);
        assertPartitionsOrder(context1, List.of(fooUnresolved, barUnresolved, zarUnresolved));

        // The response is sent back to create the session.
        int sessionId = updateAndGenerateResponseDataSessionId(context1);

        // The metadata cache only knows about foo.
        FetchContext context2 = newContext(new FetchMetadata(sessionId, 1), List.of(), fetchManager, Map.of(foo.topicId(), foo.topic()));

        // So foo is resolved but not the others.
        assertInstanceOf(IncrementalFetchContext.class, context2);
        assertPartitionsOrder(context2, List.of(foo, barUnresolved, zarUnresolved));

        updateAndGenerateResponseDataSessionId(context2);

        // The metadata cache knows about foo and bar.
        FetchContext context3 = newContext(
            new FetchMetadata(sessionId, 2),
            List.of(bar),
            fetchManager,
            Map.of(foo.topicId(), foo.topic(), bar.topicId(), bar.topic())
        );

        // So foo and bar are resolved.
        assertInstanceOf(IncrementalFetchContext.class, context3);
        assertPartitionsOrder(context3, List.of(foo, bar, zarUnresolved));

        updateAndGenerateResponseDataSessionId(context3);

        // The metadata cache knows about all topics.
        FetchContext context4 = newContext(
            new FetchMetadata(sessionId, 3),
            List.of(),
            fetchManager,
            Map.of(foo.topicId(), foo.topic(), bar.topicId(), bar.topic(), zar.topicId(), zar.topic())
        );

        // So all topics are resolved.
        assertInstanceOf(IncrementalFetchContext.class, context4);
        assertPartitionsOrder(context4, List.of(foo, bar, zar));

        updateAndGenerateResponseDataSessionId(context4);

        // The metadata cache does not know about the topics anymore (e.g. deleted).
        FetchContext context5 = newContext(new FetchMetadata(sessionId, 4), List.of(), fetchManager, Map.of());

        // All topics remain resolved.
        assertInstanceOf(IncrementalFetchContext.class, context5);
        assertPartitionsOrder(context4, List.of(foo, bar, zar));
    }

    // This test simulates trying to forget a topic partition with all possible topic ID usages for both requests.
    @ParameterizedTest
    @MethodSource({("idUsageCombinations")})
    public void testToForgetPartitions(boolean fooStartsResolved, boolean fooEndsResolved) {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);

        TopicIdPartition foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));

        TopicIdPartition fooUnresolved = new TopicIdPartition(foo.topicId(), new TopicPartition(null, foo.partition()));
        TopicIdPartition barUnresolved = new TopicIdPartition(bar.topicId(), new TopicPartition(null, bar.partition()));

        // Create a new context where foo's resolution depends on fooStartsResolved and bar is unresolved.
        Map<Uuid, String> context1Names = fooStartsResolved ? Map.of(foo.topicId(), foo.topic()) : Map.of();
        TopicIdPartition fooContext1 = fooStartsResolved ? foo : fooUnresolved;
        FetchContext context1 = newContext(
            INITIAL,
            List.of(fooContext1, bar),
            List.of(),
            fetchManager,
            context1Names
        );

        // So the context contains unresolved bar and a resolved foo iff fooStartsResolved
        assertInstanceOf(FullFetchContext.class, context1);
        assertPartitionsOrder(context1, List.of(fooContext1, barUnresolved));

        // The response is sent back to create the session.
        int sessionId = updateAndGenerateResponseDataSessionId(context1);

        // Forget foo, but keep bar. Foo's resolution depends on fooEndsResolved and bar stays unresolved.
        Map<Uuid, String> context2Names = fooEndsResolved ? Map.of(foo.topicId(), foo.topic()) : Map.of();
        TopicIdPartition fooContext2 = fooEndsResolved ? foo : fooUnresolved;
        FetchContext context2 = newContext(
            new FetchMetadata(sessionId, 1),
            List.of(),
            List.of(fooContext2),
            fetchManager,
            context2Names
        );

        // So foo is removed but not the others.
        assertInstanceOf(IncrementalFetchContext.class, context2);
        assertPartitionsOrder(context2, List.of(barUnresolved));

        updateAndGenerateResponseDataSessionId(context2);

        // Now remove bar
        FetchContext context3 = newContext(
            new FetchMetadata(sessionId, 2),
            List.of(),
            List.of(bar),
            fetchManager,
            Map.of()
        );

        // Context is sessionless since it is empty.
        assertInstanceOf(SessionlessFetchContext.class, context3);
        assertPartitionsOrder(context3, List.of());
    }

    @Test
    public void testUpdateAndGenerateResponseData() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);

        // Give both topics errors so they will stay in the session.
        TopicIdPartition foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));

        // Foo will always be resolved and bar will always not be resolved on the receiving broker.
        Map<Uuid, String> receivingBrokerTopicNames = Map.of(foo.topicId(), foo.topic());
        // The sender will know both topics' id to name mappings.
        Map<Uuid, String> sendingTopicNames = Map.of(foo.topicId(), foo.topic(), bar.topicId(), bar.topic());

        // Start with a sessionless context.
        FetchContext context1 = newContext(
            FetchMetadata.LEGACY,
            List.of(foo, bar),
            fetchManager,
            receivingBrokerTopicNames
        );
        assertInstanceOf(SessionlessFetchContext.class, context1);
        // Check the response can be read as expected.
        checkResponseData(
            Map.of(
                foo.topicPartition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                bar.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code()
            ),
            updateAndGenerateResponseData(context1),
            sendingTopicNames
        );

        // Now create a full context.
        FetchContext context2 = newContext(
            INITIAL,
            List.of(foo, bar),
            fetchManager,
            receivingBrokerTopicNames
        );
        assertInstanceOf(FullFetchContext.class, context2);

        // We want to get the session ID to build more contexts in this session.
        FetchResponse response2 = updateAndGenerateResponseData(context2);
        int sessionId = response2.sessionId();
        checkResponseData(
            Map.of(
                foo.topicPartition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                bar.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code()
            ),
            response2,
            sendingTopicNames
        );

        // Now create an incremental context. We re-add foo as though the partition data is updated. In a real broker, the data would update.
        FetchContext context3 = newContext(
            new FetchMetadata(sessionId, 1),
            List.of(),
            fetchManager,
            receivingBrokerTopicNames
        );
        assertInstanceOf(IncrementalFetchContext.class, context3);
        checkResponseData(
            Map.of(
                foo.topicPartition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(),
                bar.topicPartition(), Errors.UNKNOWN_TOPIC_ID.code()
            ),
            updateAndGenerateResponseData(context3),
            sendingTopicNames
        );

        // Finally create an error context by using the same epoch
        FetchContext context4 = newContext(
            new FetchMetadata(sessionId, 1),
            List.of(),
            fetchManager,
            receivingBrokerTopicNames
        );
        assertInstanceOf(SessionErrorContext.class, context4);
        // The response should be empty.
        assertEquals(List.of(), updateAndGenerateResponseData(context4).data().responses());
    }

    @Test
    public void testFetchSessionExpiration() {
        MockTime time = new MockTime();
        // set maximum entries to 2 to allow for eviction later
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(2, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(time, cacheShard);
        Uuid fooId = Uuid.randomUuid();
        Map<Uuid, String> topicNames = Map.of(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new fetch session, session 1
        LinkedHashMap<TopicPartition, PartitionData> session1req = new LinkedHashMap<>();
        session1req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session1req.put(foo1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest session1request1 = createRequest(INITIAL, session1req, List.of(), false, FETCH.latestVersion());
        FetchContext session1context1 = newContext(fetchManager, session1request1, topicNames);
        assertInstanceOf(FullFetchContext.class, session1context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData1.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session1resp = session1context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, session1resp.error());
        assertTrue(session1resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session1resp.responseData(topicNames, session1request1.version()).size());

        // check session entered into case
        assertTrue(cacheShard.get(session1resp.sessionId()).isPresent());

        time.sleep(500);

        // Create a second new fetch session
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session2req = new LinkedHashMap<>();
        session2req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session2req.put(foo1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest session2request1 = createRequest(INITIAL, session2req, List.of(), false, FETCH.latestVersion());
        FetchContext session2context = newContext(fetchManager, session2request1, topicNames);
        assertInstanceOf(FullFetchContext.class, session2context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> session2RespData = new LinkedHashMap<>();
        session2RespData.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        session2RespData.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session2resp = session2context.updateAndGenerateResponseData(session2RespData, List.of());
        assertEquals(Errors.NONE, session2resp.error());
        assertTrue(session2resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session2resp.responseData(topicNames, session2request1.version()).size());

        // both newly created entries are present in cache
        assertTrue(cacheShard.get(session1resp.sessionId()).isPresent());
        assertTrue(cacheShard.get(session2resp.sessionId()).isPresent());

        time.sleep(500);

        // Create an incremental fetch request for session 1
        FetchRequest session1request2 = createRequest(new FetchMetadata(session1resp.sessionId(), 1), new LinkedHashMap<>(),
            List.of(), false, FETCH.latestVersion());
        FetchContext context1v2 = newContext(fetchManager, session1request2, topicNames);
        assertInstanceOf(IncrementalFetchContext.class, context1v2);

        // total sleep time will now be large enough that fetch session 1 will be evicted if not correctly touched
        time.sleep(501);

        // create one final session to test that the least recently used entry is evicted
        // the second session should be evicted because the first session was incrementally fetched
        // more recently than the second session was created
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session3req = new LinkedHashMap<>();
        session3req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session3req.put(foo1.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        FetchRequest session3request1 = createRequest(INITIAL, session3req, List.of(), false, FETCH.latestVersion());
        FetchContext session3context = newContext(fetchManager, session3request1, topicNames);
        assertInstanceOf(FullFetchContext.class, session3context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(new TopicIdPartition(fooId, new TopicPartition("foo", 0)), new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData3.put(new TopicIdPartition(fooId, new TopicPartition("foo", 1)), new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session3resp = session3context.updateAndGenerateResponseData(respData3, List.of());
        assertEquals(Errors.NONE, session3resp.error());
        assertTrue(session3resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session3resp.responseData(topicNames, session3request1.version()).size());

        assertTrue(cacheShard.get(session1resp.sessionId()).isPresent());
        assertFalse(cacheShard.get(session2resp.sessionId()).isPresent(),
            "session 2 should have been evicted by latest session, as session 1 was used more recently");
        assertTrue(cacheShard.get(session3resp.sessionId()).isPresent());
    }

    @Test
    public void testPrivilegedSessionHandling() {
        MockTime time = new MockTime();
        // set maximum entries to 2 to allow for eviction later
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(2, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(time, cacheShard);
        Uuid fooId = Uuid.randomUuid();
        Map<Uuid, String> topicNames = Map.of(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new fetch session, session 1
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session1req = new LinkedHashMap<>();
        session1req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session1req.put(foo1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest session1request = createRequest(INITIAL, session1req, List.of(), true, FETCH.latestVersion());
        FetchContext session1context = newContext(fetchManager, session1request, topicNames);
        assertInstanceOf(FullFetchContext.class, session1context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData1.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session1resp = session1context.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, session1resp.error());
        assertTrue(session1resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session1resp.responseData(topicNames, session1request.version()).size());
        assertEquals(1, cacheShard.size());

        // move time forward to age session 1 a little compared to session 2
        time.sleep(500);

        // Create a second new fetch session, unprivileged
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session2req = new LinkedHashMap<>();
        session2req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session2req.put(foo1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest session2request = createRequest(INITIAL, session2req, List.of(), false, FETCH.latestVersion());
        FetchContext session2context = newContext(fetchManager, session2request, topicNames);
        assertInstanceOf(FullFetchContext.class, session2context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> session2RespData = new LinkedHashMap<>();
        session2RespData.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        session2RespData.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session2resp = session2context.updateAndGenerateResponseData(session2RespData, List.of());
        assertEquals(Errors.NONE, session2resp.error());
        assertTrue(session2resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session2resp.responseData(topicNames, session2request.version()).size());

        // both newly created entries are present in cache
        assertTrue(cacheShard.get(session1resp.sessionId()).isPresent());
        assertTrue(cacheShard.get(session2resp.sessionId()).isPresent());
        assertEquals(2, cacheShard.size());

        time.sleep(500);

        // create a session to test session1 privileges mean that session 1 is retained and session 2 is evicted
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session3req = new LinkedHashMap<>();
        session3req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session3req.put(foo1.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        FetchRequest session3request = createRequest(INITIAL, session3req, List.of(), true, FETCH.latestVersion());
        FetchContext session3context = newContext(fetchManager, session3request, topicNames);
        assertInstanceOf(FullFetchContext.class, session3context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData3.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session3resp = session3context.updateAndGenerateResponseData(respData3, List.of());
        assertEquals(Errors.NONE, session3resp.error());
        assertTrue(session3resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session3resp.responseData(topicNames, session3request.version()).size());

        assertTrue(cacheShard.get(session1resp.sessionId()).isPresent());
        // even though session 2 is more recent than session 1, and has not reached expiry time, it is less
        // privileged than session 2, and thus session 3 should be entered and session 2 evicted.
        assertFalse(cacheShard.get(session2resp.sessionId()).isPresent(), "session 2 should have been evicted by session 3");
        assertTrue(cacheShard.get(session3resp.sessionId()).isPresent());
        assertEquals(2, cacheShard.size());

        time.sleep(501);

        // create a final session to test whether session1 can be evicted due to age even though it is privileged
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> session4req = new LinkedHashMap<>();
        session4req.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        session4req.put(foo1.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        FetchRequest session4request = createRequest(INITIAL, session4req, List.of(), true, FETCH.latestVersion());
        FetchContext session4context = newContext(fetchManager, session4request, topicNames);
        assertInstanceOf(FullFetchContext.class, session4context);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData4 = new LinkedHashMap<>();
        respData4.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData4.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse session4resp = session3context.updateAndGenerateResponseData(respData4, List.of());
        assertEquals(Errors.NONE, session4resp.error());
        assertTrue(session4resp.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, session4resp.responseData(topicNames, session4request.version()).size());

        assertFalse(cacheShard.get(session1resp.sessionId()).isPresent(),
            "session 1 should have been evicted by session 4 even though it is privileged as it has hit eviction time");
        assertTrue(cacheShard.get(session3resp.sessionId()).isPresent());
        assertTrue(cacheShard.get(session4resp.sessionId()).isPresent());
        assertEquals(2, cacheShard.size());
    }

    @Test
    public void testZeroSizeFetchSession() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Uuid fooId = Uuid.randomUuid();
        Map<Uuid, String> topicNames = Map.of(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new fetch session with foo-0 and foo-1
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0.topicPartition(), new PartitionData(fooId, 0, 0, 100, Optional.empty()));
        reqData1.put(foo1.topicPartition(), new PartitionData(fooId, 10, 0, 100, Optional.empty()));
        FetchRequest request1 = createRequest(INITIAL, reqData1, List.of(), false, FETCH.latestVersion());
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(100)
            .setLastStableOffset(100)
            .setLogStartOffset(100));
        respData1.put(foo1, new FetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setHighWatermark(10)
            .setLastStableOffset(10)
            .setLogStartOffset(10));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertTrue(resp1.sessionId() != INVALID_SESSION_ID);
        assertEquals(2, resp1.responseData(topicNames, request1.version()).size());

        // Create an incremental fetch request that removes foo-0 and foo-1
        // Verify that the previous fetch session was closed.
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), new LinkedHashMap<>(),
            List.of(foo0, foo1), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertInstanceOf(SessionlessFetchContext.class, context2);

        FetchResponse resp2 = context2.updateAndGenerateResponseData(new LinkedHashMap<>(), List.of());
        assertEquals(INVALID_SESSION_ID, resp2.sessionId());
        assertTrue(resp2.responseData(topicNames, request2.version()).isEmpty());
        assertEquals(0, cacheShard.size());
    }

    @Test
    public void testDivergingEpoch() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<Uuid, String> topicNames = Map.of(Uuid.randomUuid(), "foo", Uuid.randomUuid(), "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 2));

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqData = new LinkedHashMap<>();
        reqData.put(tp1.topicPartition(), new PartitionData(tp1.topicId(), 100, 0, 1000, Optional.of(5), Optional.of(4)));
        reqData.put(tp2.topicPartition(), new PartitionData(tp2.topicId(), 100, 0, 1000, Optional.of(5), Optional.of(4)));

        // Full fetch context returns all partitions in the response
        FetchRequest request1 = createRequest(INITIAL, reqData, List.of(), false, FETCH.latestVersion());
        FetchContext context1 = newContext(fetchManager, request1, topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData = new LinkedHashMap<>();
        respData.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition())
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0));
        FetchResponseData.EpochEndOffset divergingEpoch = new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(90);
        respData.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp2.partition())
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setDivergingEpoch(divergingEpoch));
        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertNotEquals(INVALID_SESSION_ID, resp1.sessionId());
        assertEquals(Set.of(tp1.topicPartition(), tp2.topicPartition()), resp1.responseData(topicNames, request1.version()).keySet());

        // Incremental fetch context returns partitions with divergent epoch even if none
        // of the other conditions for return are met.
        FetchRequest request2 = createRequest(new FetchMetadata(resp1.sessionId(), 1), reqData, List.of(), false, FETCH.latestVersion());
        FetchContext context2 = newContext(fetchManager, request2, topicNames);
        assertInstanceOf(IncrementalFetchContext.class, context2);

        FetchResponse resp2 = context2.updateAndGenerateResponseData(respData, List.of());
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(resp1.sessionId(), resp2.sessionId());
        assertEquals(Set.of(tp2.topicPartition()), resp2.responseData(topicNames, request2.version()).keySet());

        // All partitions with divergent epoch should be returned.
        respData.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition())
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setDivergingEpoch(divergingEpoch));
        FetchResponse resp3 = context2.updateAndGenerateResponseData(respData, List.of());
        assertEquals(Errors.NONE, resp3.error());
        assertEquals(resp1.sessionId(), resp3.sessionId());
        assertEquals(Set.of(tp1.topicPartition(), tp2.topicPartition()), resp3.responseData(topicNames, request2.version()).keySet());

        // Partitions that meet other conditions should be returned regardless of whether
        // divergingEpoch is set or not.
        respData.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.partition())
            .setHighWatermark(110)
            .setLastStableOffset(110)
            .setLogStartOffset(0));
        FetchResponse resp4 = context2.updateAndGenerateResponseData(respData, List.of());
        assertEquals(Errors.NONE, resp4.error());
        assertEquals(resp1.sessionId(), resp4.sessionId());
        assertEquals(Set.of(tp1.topicPartition(), tp2.topicPartition()), resp4.responseData(topicNames, request2.version()).keySet());
    }

    @Test
    public void testDeprioritizesPartitionsWithRecordsOnly() {
        FetchSessionCacheShard cacheShard = new FetchSessionCacheShard(10, 1000, Integer.MAX_VALUE, 0);
        FetchManager fetchManager = new FetchManager(new MockTime(), cacheShard);
        Map<String, Uuid> topicIds = Map.of("foo", Uuid.randomUuid(), "bar", Uuid.randomUuid(), "zar", Uuid.randomUuid());
        Map<Uuid, String> topicNames = topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp1 = new TopicIdPartition(topicIds.get("foo"), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicIds.get("bar"), new TopicPartition("bar", 2));
        TopicIdPartition tp3 = new TopicIdPartition(topicIds.get("zar"), new TopicPartition("zar", 3));

        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> reqData = new LinkedHashMap<>();
        reqData.put(tp1, new PartitionData(tp1.topicId(), 100, 0, 1000, Optional.of(5), Optional.of(4)));
        reqData.put(tp2, new PartitionData(tp2.topicId(), 100, 0, 1000, Optional.of(5), Optional.of(4)));
        reqData.put(tp3, new PartitionData(tp3.topicId(), 100, 0, 1000, Optional.of(5), Optional.of(4)));

        // Full fetch context returns all partitions in the response
        FetchContext context1 = fetchManager.newContext(FETCH.latestVersion(), INITIAL, false, reqData, List.of(), topicNames);
        assertInstanceOf(FullFetchContext.class, context1);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.topicPartition().partition())
            .setHighWatermark(50)
            .setLastStableOffset(50)
            .setLogStartOffset(0));
        respData1.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp2.topicPartition().partition())
            .setHighWatermark(50)
            .setLastStableOffset(50)
            .setLogStartOffset(0));
        respData1.put(tp3, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp3.topicPartition().partition())
            .setHighWatermark(50)
            .setLastStableOffset(50)
            .setLogStartOffset(0));

        FetchResponse resp1 = context1.updateAndGenerateResponseData(respData1, List.of());
        assertEquals(Errors.NONE, resp1.error());
        assertNotEquals(INVALID_SESSION_ID, resp1.sessionId());
        assertEquals(
            Set.of(tp1.topicPartition(), tp2.topicPartition(), tp3.topicPartition()),
            resp1.responseData(topicNames, FETCH.latestVersion()).keySet()
        );

        // Incremental fetch context returns partitions with changes but only deprioritizes
        // the partitions with records
        FetchContext context2 = fetchManager.newContext(FETCH.latestVersion(), new FetchMetadata(resp1.sessionId(), 1),
            false, reqData, List.of(), topicNames);
        assertInstanceOf(IncrementalFetchContext.class, context2);

        // Partitions are ordered in the session as per last response
        assertPartitionsOrder(context2, List.of(tp1, tp2, tp3));

        // Response is empty
        FetchResponse resp2 = context2.updateAndGenerateResponseData(new LinkedHashMap<>(), List.of());
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(resp1.sessionId(), resp2.sessionId());
        assertEquals(Set.of(), resp2.responseData(topicNames, FETCH.latestVersion()).keySet());

        // All partitions with changes should be returned.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(tp1, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp1.topicPartition().partition())
            .setHighWatermark(60)
            .setLastStableOffset(50)
            .setLogStartOffset(0));
        respData3.put(tp2, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp2.topicPartition().partition())
            .setHighWatermark(60)
            .setLastStableOffset(50)
            .setLogStartOffset(0)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(100, null))));
        respData3.put(tp3, new FetchResponseData.PartitionData()
            .setPartitionIndex(tp3.topicPartition().partition())
            .setHighWatermark(50)
            .setLastStableOffset(50)
            .setLogStartOffset(0));
        FetchResponse resp3 = context2.updateAndGenerateResponseData(respData3, List.of());
        assertEquals(Errors.NONE, resp3.error());
        assertEquals(resp1.sessionId(), resp3.sessionId());
        assertEquals(Set.of(tp1.topicPartition(), tp2.topicPartition()), resp3.responseData(topicNames, FETCH.latestVersion()).keySet());

        // Only the partitions whose returned records in the last response
        // were deprioritized
        assertPartitionsOrder(context2, List.of(tp1, tp3, tp2));
    }

    @Test
    public void testCachedPartitionEqualsAndHashCode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic";
        int partition = 0;

        CachedPartition cachedPartitionWithIdAndName = new CachedPartition(topicName, topicId, partition);
        CachedPartition cachedPartitionWithIdAndNoName = new CachedPartition(null, topicId, partition);
        CachedPartition cachedPartitionWithDifferentIdAndName = new CachedPartition(topicName, Uuid.randomUuid(), partition);
        CachedPartition cachedPartitionWithZeroIdAndName = new CachedPartition(topicName, Uuid.ZERO_UUID, partition);
        CachedPartition cachedPartitionWithZeroIdAndOtherName = new CachedPartition("otherTopic", Uuid.ZERO_UUID, partition);

        // CachedPartitions with valid topic IDs will compare topic ID and partition but not topic name.
        assertEquals(cachedPartitionWithIdAndName, cachedPartitionWithIdAndNoName);
        assertEquals(cachedPartitionWithIdAndName.hashCode(), cachedPartitionWithIdAndNoName.hashCode());

        assertNotEquals(cachedPartitionWithIdAndName, cachedPartitionWithDifferentIdAndName);
        assertNotEquals(cachedPartitionWithIdAndName.hashCode(), cachedPartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedPartitionWithIdAndName, cachedPartitionWithZeroIdAndName);
        assertNotEquals(cachedPartitionWithIdAndName.hashCode(), cachedPartitionWithZeroIdAndName.hashCode());

        // CachedPartitions will null name and valid IDs will act just like ones with valid names
        assertEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithIdAndName);
        assertEquals(cachedPartitionWithIdAndNoName.hashCode(), cachedPartitionWithIdAndName.hashCode());

        assertNotEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithDifferentIdAndName);
        assertNotEquals(cachedPartitionWithIdAndNoName.hashCode(), cachedPartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedPartitionWithIdAndNoName, cachedPartitionWithZeroIdAndName);
        assertNotEquals(cachedPartitionWithIdAndNoName.hashCode(), cachedPartitionWithZeroIdAndName.hashCode());

        // CachedPartition with zero Uuids will compare topic name and partition.
        assertNotEquals(cachedPartitionWithZeroIdAndName, cachedPartitionWithZeroIdAndOtherName);
        assertNotEquals(cachedPartitionWithZeroIdAndName.hashCode(), cachedPartitionWithZeroIdAndOtherName.hashCode());

        assertEquals(cachedPartitionWithZeroIdAndName, cachedPartitionWithZeroIdAndName);
        assertEquals(cachedPartitionWithZeroIdAndName.hashCode(), cachedPartitionWithZeroIdAndName.hashCode());
    }

    @Test
    public void testMaybeResolveUnknownName() {
        CachedPartition namedPartition = new CachedPartition("topic", Uuid.randomUuid(), 0);
        CachedPartition nullNamePartition1 = new CachedPartition(null, Uuid.randomUuid(), 0);
        CachedPartition nullNamePartition2 = new CachedPartition(null, Uuid.randomUuid(), 0);
        Map<Uuid, String> topicNames = Map.of(namedPartition.topicId(), "foo", nullNamePartition1.topicId(), "bar");

        // Since the name is not null, we should not change the topic name.
        // We should never have a scenario where the same ID is used by two topic names, but this is used to test we respect the null check.
        namedPartition.maybeResolveUnknownName(topicNames);
        assertEquals("topic", namedPartition.topic());

        // We will resolve this name as it is in the map and the current name is null.
        nullNamePartition1.maybeResolveUnknownName(topicNames);
        assertEquals("bar", nullNamePartition1.topic());

        // If the ID is not in the map, then we don't resolve the name.
        nullNamePartition2.maybeResolveUnknownName(topicNames);
        assertNull(nullNamePartition2.topic());
    }

    @Test
    public void testFetchSessionCache_getShardedCache_retrievesCacheFromCorrectSegment() {
        // Given
        int numShards = 8;
        int sessionIdRange = Integer.MAX_VALUE / numShards;
        List<FetchSessionCacheShard> cacheShards = IntStream.range(0, numShards)
            .mapToObj(shardNum -> new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
            .toList();
        FetchSessionCache cache = new FetchSessionCache(cacheShards);

        // When
        FetchSessionCacheShard cache0 = cache.getCacheShard(sessionIdRange - 1);
        FetchSessionCacheShard cache1 = cache.getCacheShard(sessionIdRange);
        FetchSessionCacheShard cache2 = cache.getCacheShard(sessionIdRange * 2);

        // Then
        assertEquals(cache0, cacheShards.get(0));
        assertEquals(cache1, cacheShards.get(1));
        assertEquals(cache2, cacheShards.get(2));
        assertThrows(IndexOutOfBoundsException.class, () -> cache.getCacheShard(sessionIdRange * numShards));
    }

    @Test
    public void testFetchSessionCache_RoundRobinsIntoShards() {
        // Given
        int numShards = 8;
        int sessionIdRange = Integer.MAX_VALUE / numShards;
        List<FetchSessionCacheShard> cacheShards = IntStream.range(0, numShards)
            .mapToObj(shardNum -> new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
            .toList();
        FetchSessionCache cache = new FetchSessionCache(cacheShards);

        // When / Then
        for (int shardNum = 0; shardNum < numShards * 2; shardNum++)
            assertEquals(cacheShards.get(shardNum % numShards), cache.getNextCacheShard());
    }

    @Test
    public void testFetchSessionCache_RoundRobinsIntoShards_WhenIntegerOverflows() {
        // Given
        int maxInteger = Integer.MAX_VALUE;
        FetchSessionCache.COUNTER.set(maxInteger + 1);
        int numShards = 8;
        int sessionIdRange = Integer.MAX_VALUE / numShards;
        List<FetchSessionCacheShard> cacheShards = IntStream.range(0, numShards)
            .mapToObj(shardNum -> new FetchSessionCacheShard(10, 1000, sessionIdRange, shardNum))
            .toList();
        FetchSessionCache cache = new FetchSessionCache(cacheShards);

        // When / Then
        for (int shardNum = 0; shardNum < numShards * 2; shardNum++)
            assertEquals(cacheShards.get(shardNum % numShards), cache.getNextCacheShard());
    }

    private void assertCacheContains(FetchSessionCacheShard cacheShard, int... sessionIds) {
        int i = 0;
        for (int sessionId : sessionIds) {
            i = i + 1;
            assertTrue(cacheShard.get(sessionId).isPresent(),
                "Missing session " + i + " out of " + List.of(sessionIds).size() + " " + sessionId + "\"");
        }
        assertEquals(sessionIds.length, cacheShard.size());
    }

    private ImplicitLinkedHashCollection<CachedPartition> createPartitions(int size) {
        ImplicitLinkedHashCollection<CachedPartition> cacheMap = new ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++)
            cacheMap.add(new CachedPartition("test", Uuid.randomUuid(), i));
        return cacheMap;
    }

    private FetchRequest createRequest(FetchMetadata metadata,
                                       Map<TopicPartition, PartitionData> fetchData,
                                       List<TopicIdPartition> toForget,
                                       boolean isFromFollower,
                                       short version) {
        return new FetchRequest.Builder(
            version,
            version,
            isFromFollower ? 1 : FetchRequest.CONSUMER_REPLICA_ID,
            isFromFollower ? 1 : -1,
            0,
            0,
            fetchData)
        .metadata(metadata)
        .removed(toForget)
        .build();
    }

    private FetchRequest createRequestWithoutTopicIds(FetchMetadata metadata,
                                                      Map<TopicPartition, PartitionData> fetchData) {
        return new FetchRequest.Builder(
            (short) 12,
            (short) 12,
            FetchRequest.CONSUMER_REPLICA_ID,
            -1,
            0,
            0,
            fetchData)
        .metadata(metadata)
        .removed(List.of())
        .build();
    }

    private FetchContext newContext(FetchManager fetchManager, FetchRequest request, Map<Uuid, String> topicNames) {
        return fetchManager.newContext(
            request.version(),
            request.metadata(),
            request.isFromFollower(),
            request.fetchData(topicNames),
            request.forgottenTopics(topicNames),
            topicNames
        );
    }

    private FetchContext newContext(FetchMetadata metadata,
                                    List<TopicIdPartition> partitions,
                                    FetchManager fetchManager,
                                    Map<Uuid, String> topicNames) { // Topic ID to name mapping known by the broker.
        LinkedHashMap<TopicPartition, PartitionData> data = new LinkedHashMap<>();

        partitions.forEach(topicIdPartition ->
            data.put(
                topicIdPartition.topicPartition(),
                new PartitionData(topicIdPartition.topicId(), 0, 0, 100, Optional.empty())
            )
        );

        FetchRequest fetchRequest = createRequest(metadata, data, List.of(), false, FETCH.latestVersion());

        return fetchManager.newContext(
            fetchRequest.version(),
            fetchRequest.metadata(),
            fetchRequest.isFromFollower(),
            fetchRequest.fetchData(topicNames),
            fetchRequest.forgottenTopics(topicNames),
            topicNames
        );
    }

    private FetchContext newContext(FetchMetadata metadata,
                                    List<TopicIdPartition> partitions,
                                    List<TopicIdPartition> toForget,
                                    FetchManager fetchManager,
                                    Map<Uuid, String> topicNames) { // Topic ID to name mapping known by the broker.
        LinkedHashMap<TopicPartition, PartitionData> data = new LinkedHashMap<>();

        partitions.forEach(topicIdPartition ->
            data.put(
                topicIdPartition.topicPartition(),
                new PartitionData(topicIdPartition.topicId(), 0, 0, 100, Optional.empty())
            )
        );

        FetchRequest fetchRequest = createRequest(metadata, data, toForget, false, FETCH.latestVersion());

        return fetchManager.newContext(
            fetchRequest.version(),
            fetchRequest.metadata(),
            fetchRequest.isFromFollower(),
            fetchRequest.fetchData(topicNames),
            fetchRequest.forgottenTopics(topicNames),
            topicNames
        );
    }

    private Map<TopicIdPartition, Optional<Integer>> cachedLeaderEpochs(FetchContext context) {
        Map<TopicIdPartition, Optional<Integer>> map = new HashMap<>();
        context.foreachPartition((tp, data) -> map.put(tp, data.currentLeaderEpoch));
        return map;
    }

    private Map<TopicIdPartition, Optional<Integer>> cachedLastFetchedEpochs(FetchContext context) {
        Map<TopicIdPartition, Optional<Integer>> map = new HashMap<>();
        context.foreachPartition((tp, data) -> map.put(tp, data.lastFetchedEpoch));
        return map;
    }

    private FetchResponseData.PartitionData errorResponse(short errorCode) {
        return new FetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setHighWatermark(-1)
            .setLastStableOffset(-1)
            .setLogStartOffset(-1)
            .setErrorCode(errorCode);
    }

    private int updateAndGenerateResponseDataSessionId(FetchContext context) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> data = new LinkedHashMap<>();

        context.foreachPartition((topicIdPartition, partData) ->
            data.put(
                topicIdPartition,
                topicIdPartition.topic() == null
                    ? errorResponse(Errors.UNKNOWN_TOPIC_ID.code())
                    : new FetchResponseData.PartitionData()
                        .setPartitionIndex(1)
                        .setHighWatermark(10)
                        .setLastStableOffset(10)
                        .setLogStartOffset(10)
            )
        );

        return context.updateAndGenerateResponseData(data, List.of()).sessionId();
    }

    private FetchResponse updateAndGenerateResponseData(FetchContext context) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> data = new LinkedHashMap<>();

        context.foreachPartition((topicIdPartition, partData) ->
            data.put(
                topicIdPartition,
                topicIdPartition.topic() == null
                    ? errorResponse(Errors.UNKNOWN_TOPIC_ID.code())
                    : errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
            )
        );

        return context.updateAndGenerateResponseData(data, List.of());
    }

    private void checkResponseData(Map<TopicPartition, Short> expected, FetchResponse response, Map<Uuid, String> topicNames) {
        assertEquals(
            expected,
            response.responseData(topicNames, FETCH.latestVersion()).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().errorCode()))
        );
    }

    private void assertPartitionsOrder(FetchContext context, List<TopicIdPartition> partitions) {
        List<TopicIdPartition> partitionsInContext = new ArrayList<>();
        context.foreachPartition((tp, data) -> partitionsInContext.add(tp));
        assertEquals(partitions, partitionsInContext);
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> idUsageCombinations() {
        List<Arguments> data = new ArrayList<>();
        List<Boolean> params = List.of(Boolean.TRUE, Boolean.FALSE);

        for (boolean startsWithTopicIds : params)
            for (boolean endsWithTopicIds : params)
                data.add(Arguments.of(startsWithTopicIds, endsWithTopicIds));

        return data.stream();
    }
}
