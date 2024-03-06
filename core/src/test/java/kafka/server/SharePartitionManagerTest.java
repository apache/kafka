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
package kafka.server;

import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class SharePartitionManagerTest {

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
    public void testNewContextReturnsFinalContext() {
        SharePartitionManager sharePartitionManager = new SharePartitionManager(Mockito.mock(ReplicaManager.class),
                new MockTime(), new SharePartitionManager.ShareSessionCache(10, 1000));

        ShareFetchMetadata newReqMetadata = new ShareFetchMetadata(Uuid.ZERO_UUID, -1);
        ShareFetchContext shareFetchContext = sharePartitionManager.newContext("grp", new HashMap<>(), new ArrayList<>(),
                new HashMap<>(), newReqMetadata);
        assertEquals(shareFetchContext.getClass(), SharePartitionManager.FinalContext.class);
    }

    private ImplicitLinkedHashCollection<SharePartitionManager.CachedSharePartition> dummyCreate(int size) {
        ImplicitLinkedHashCollection<SharePartitionManager.CachedSharePartition> cacheMap = new
                ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++)
            cacheMap.add(new SharePartitionManager.CachedSharePartition("test", Uuid.randomUuid(), i));
        return cacheMap;
    }

    public void assertShareCacheContains(SharePartitionManager.ShareSessionCache cache,
                                         ArrayList<SharePartitionManager.ShareSessionKey> sessionKeys) {
        int i = 0;
        for (SharePartitionManager.ShareSessionKey sessionKey : sessionKeys) {
            i++;
            assertFalse(cache.get(sessionKey).isEmpty(),
                    "Missing session " + i + " out of " + sessionKeys.size() + " ( " + sessionKey + " )");
        }
        assertEquals(sessionKeys.size(), cache.size());
    }

    @Test
    public void testShareSessionCache() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(3, 100);
        assertEquals(0, cache.size());
        SharePartitionManager.ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                10, dummyCreate(10));
        SharePartitionManager.ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 10,
                20, dummyCreate(20));
        SharePartitionManager.ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 20,
                30, dummyCreate(30));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 30,
                40, dummyCreate(40)));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 40,
                5, dummyCreate(5)));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2, key3)));
        cache.touch(cache.get(key1), 200);
        SharePartitionManager.ShareSessionKey key4 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 210,
                11, dummyCreate(11));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key3, key4)));
        cache.touch(cache.get(key1), 400);
        cache.touch(cache.get(key3), 390);
        cache.touch(cache.get(key4), 400);
        SharePartitionManager.ShareSessionKey key5 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 410,
                50, dummyCreate(50));
        assertNull(key5);
    }

    @Test
    public void testResizeCachedSessions() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(2, 100);
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
        SharePartitionManager.ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                2, dummyCreate(2));
        assertNotNull(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key1)));
        SharePartitionManager.ShareSession session1 = cache.get(key1);
        assertEquals(2, session1.size());
        assertEquals(2, cache.totalPartitions());
        assertEquals(1, cache.size());

        SharePartitionManager.ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                4, dummyCreate(4));
        assertNotNull(key2);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        SharePartitionManager.ShareSession session2 = cache.get(key2);
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.touch(session1, 200);
        cache.touch(session2, 200);

        SharePartitionManager.ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 200,
                5, dummyCreate(5));
        assertNull(key3);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.remove(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key2)));
        assertEquals(1, cache.size());
        assertEquals(4, cache.totalPartitions());

        Iterator<SharePartitionManager.CachedSharePartition> iterator = session2.partitionMap().iterator();
        iterator.next();
        iterator.remove();
        assertEquals(3, session2.size());
        assertEquals(4, session2.cachedSize());
        cache.touch(session2, session2.lastUsedMs());
        assertEquals(3, cache.totalPartitions());
    }

    private final List<TopicIdPartition> emptyPartList = Collections.unmodifiableList(new ArrayList<>());

    private Map<TopicIdPartition, Optional<Integer>> cachedLeaderEpochs(ShareFetchContext context) {
        Map<TopicIdPartition, Optional<Integer>> cachedLeaderMap = new HashMap<>();
        if (context.getClass() == SharePartitionManager.FinalContext.class) {
            ((SharePartitionManager.FinalContext) context).shareFetchData().forEach((topicIdPartition, sharePartitionData) ->
                    cachedLeaderMap.put(topicIdPartition, sharePartitionData.currentLeaderEpoch));
        } else if (context.getClass() == SharePartitionManager.ShareSessionErrorContext.class) {
            return cachedLeaderMap;
        } else {
            SharePartitionManager.ShareSessionContext shareSessionContext = (SharePartitionManager.ShareSessionContext) context;
            if (!shareSessionContext.isSubsequent()) {
                shareSessionContext.shareFetchData().forEach((topicIdPartition, sharePartitionData) -> cachedLeaderMap.put(topicIdPartition, sharePartitionData.currentLeaderEpoch));
            } else {
                synchronized (shareSessionContext.session()) {
                    shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                                TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                        ShareFetchRequest.SharePartitionData reqData = cachedSharePartition.reqData();
                        cachedLeaderMap.put(topicIdPartition, reqData.currentLeaderEpoch);
                    });
                }
            }
        }
        return cachedLeaderMap;
    }

    @Test
    public void testCachedLeaderEpoch() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = new SharePartitionManager(Mockito.mock(ReplicaManager.class),
                time, cache);
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> requestData1 = new LinkedHashMap<>();
        requestData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100, Optional.empty()));
        requestData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100, Optional.of(1)));
        requestData1.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100, Optional.of(2)));

        ShareFetchMetadata reqMetadata = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        String groupId = "grp";

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, requestData1, emptyPartList,
                topicNames, reqMetadata);

        Map<TopicIdPartition, Optional<Integer>> epochs1 = cachedLeaderEpochs(context1);
        assertEquals(Optional.empty(), epochs1.get(tp0));
        assertEquals(Optional.of(1), epochs1.get(tp1));
        assertEquals(Optional.of(2), epochs1.get(tp2));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> response = new LinkedHashMap<>();
        response.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp0.partition()));
        response.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));
        response.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));

        context1.updateAndGenerateResponseData(groupId, reqMetadata.memberId(), response);

        // With no changes, the cached epochs should remain the same
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> requestData2 = new LinkedHashMap<>();

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, requestData2, emptyPartList,
                topicNames, new ShareFetchMetadata(reqMetadata.memberId(), 1));
        Map<TopicIdPartition, Optional<Integer>> epochs2 = cachedLeaderEpochs(context2);
        assertEquals(Optional.empty(), epochs2.get(tp0));
        assertEquals(Optional.of(1), epochs2.get(tp1));
        assertEquals(Optional.of(2), epochs2.get(tp2));
        context2.updateAndGenerateResponseData(groupId, reqMetadata.memberId(), response);

        // Now verify we can change the leader epoch and the context is updated
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> requestData3 = new LinkedHashMap<>();
        requestData3.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100, Optional.of(6)));
        requestData3.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100, Optional.empty()));
        requestData3.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100, Optional.of(3)));
        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, requestData3, emptyPartList,
                topicNames, new ShareFetchMetadata(reqMetadata.memberId(), 2));
        Map<TopicIdPartition, Optional<Integer>> epochs3 = cachedLeaderEpochs(context3);
        assertEquals(Optional.of(6), epochs3.get(tp0));
        assertEquals(Optional.empty(), epochs3.get(tp1));
        assertEquals(Optional.of(3), epochs3.get(tp2));
    }

    @Test
    public void testShareFetchRequests() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = new SharePartitionManager(Mockito.mock(ReplicaManager.class),
                time, cache);
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        String groupId = "grp";

        // Verify that final epoch requests get a FinalContext
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, new HashMap<>(), emptyPartList,
                topicNames, new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context1.getClass(), SharePartitionManager.FinalContext.class);

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100, Optional.empty()));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100, Optional.empty()));


        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                topicNames, reqMetadata2);
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());

        Iterator<Map.Entry<TopicIdPartition, ShareFetchRequest.SharePartitionData>> reqData2Iter = reqData2.entrySet().iterator();
        ((SharePartitionManager.ShareSessionContext) context2).shareFetchData().forEach((topicIdPartition, sharePartitionData) -> {
            Map.Entry<TopicIdPartition, ShareFetchRequest.SharePartitionData> entry = reqData2Iter.next();
            assertEquals(entry.getKey().topicPartition(), topicIdPartition.topicPartition());
            assertEquals(topicIds.get(entry.getKey().topic()), topicIdPartition.topicId());
            assertEquals(entry.getValue(), sharePartitionData);
        });

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));

        SharePartitionManager.ShareSessionKey shareSessionKey2 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Test trying to create a new session with an invalid epoch
        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                topicNames, new ShareFetchMetadata(shareSessionKey2.memberId(), 5));
        assertEquals(context3.getClass(), SharePartitionManager.ShareSessionErrorContext.class);
        assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH,
                context3.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2).error());

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        ShareFetchContext context4 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                topicNames, new ShareFetchMetadata(memberId4, 1));
        assertEquals(context4.getClass(), SharePartitionManager.ShareSessionErrorContext.class);
        assertEquals(Errors.SHARE_SESSION_NOT_FOUND,
                context4.updateAndGenerateResponseData(groupId, memberId4, respData2).error());

        // Continue the first fetch session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = new LinkedHashMap<>();
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, emptyPartList,
                topicNames, new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(context5.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context5).isSubsequent());

        Iterator<Map.Entry<TopicIdPartition, ShareFetchRequest.SharePartitionData>> reqData5Iter = reqData2.entrySet().iterator();
        SharePartitionManager.ShareSessionContext shareSessionContext5 = (SharePartitionManager.ShareSessionContext) context5;
        synchronized (shareSessionContext5.session()) {
            shareSessionContext5.session().partitionMap().forEach(cachedSharePartition -> {
                Map.Entry<TopicIdPartition, ShareFetchRequest.SharePartitionData> entry = reqData5Iter.next();
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                ShareFetchRequest.SharePartitionData data = cachedSharePartition.reqData();
                assertEquals(entry.getKey().topicPartition(), topicIdPartition.topicPartition());
                assertEquals(topicIds.get(entry.getKey().topic()), topicIdPartition.topicId());
                assertEquals(entry.getValue(), data);
            });
        }
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(0, resp5.responseData(topicNames).size());

        // Test setting an invalid share session epoch.
        ShareFetchContext context6 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                topicNames, new ShareFetchMetadata(shareSessionKey2.memberId(), 5));
        assertEquals(context6.getClass(), SharePartitionManager.ShareSessionErrorContext.class);
        assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH,
                context6.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2).error());

        // Test generating a throttled response for a subsequent share fetch session
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = new LinkedHashMap<>();
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, emptyPartList,
                topicNames, new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        // Close the subsequent fetch session.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData8 = new LinkedHashMap<>();
        reqData8.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100, Optional.empty()));
        reqData8.put(tp3, new ShareFetchRequest.SharePartitionData(tp3.topicId(), 100, Optional.empty()));
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, reqData8, emptyPartList,
                topicNames, new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context8.getClass(), SharePartitionManager.FinalContext.class);
        assertEquals(0, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
    }

    @Test
    public void testCachedSharePartitionEqualsAndHashCode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic";
        int partition = 0;

        SharePartitionManager.CachedSharePartition cachedSharePartitionWithIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, topicId, partition);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithIdAndNoName = new
                SharePartitionManager.CachedSharePartition(null, topicId, partition);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithDifferentIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, Uuid.randomUuid(), partition);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithZeroIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, Uuid.ZERO_UUID, partition);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithZeroIdAndOtherName = new
                SharePartitionManager.CachedSharePartition("otherTopic", Uuid.ZERO_UUID, partition);

        // CachedSharePartitions with valid topic IDs will compare topic ID and partition but not topic name.
        assertEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithIdAndNoName);
        assertEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithIdAndNoName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        // CachedSharePartitions will null name and valid IDs will act just like ones with valid names

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        assertEquals(cachedSharePartitionWithZeroIdAndName, cachedSharePartitionWithZeroIdAndName);
        assertEquals(cachedSharePartitionWithZeroIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());
    }
}
