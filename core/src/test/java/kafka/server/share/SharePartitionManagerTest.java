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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidShareSessionEpochException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.NoOpShareStatePersister;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.ShareSessionCache;
import org.apache.kafka.server.share.ShareSessionKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Timeout(120)
public class SharePartitionManagerTest {

    private static final int RECORD_LOCK_DURATION_MS = 30000;
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;

    private static final List<TopicIdPartition> EMPTY_PART_LIST = Collections.unmodifiableList(new ArrayList<>());

    @Test
    public void testNewContextReturnsFinalContext() {
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();

        ShareFetchMetadata newReqMetadata = new ShareFetchMetadata(Uuid.ZERO_UUID, -1);
        ShareFetchContext shareFetchContext = sharePartitionManager.newContext("grp", Collections.emptyMap(), Collections.emptyList(), newReqMetadata);
        assertEquals(FinalContext.class, shareFetchContext.getClass());

        // If the final fetch request has topics to add, it should fail as an invalid request
        Uuid topicId = Uuid.randomUuid();
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData = Collections.singletonMap(
                new TopicIdPartition(topicId, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(topicId, 4000));
        assertThrows(InvalidRequestException.class,
                () -> sharePartitionManager.newContext("grp", shareFetchData, Collections.emptyList(), new ShareFetchMetadata(Uuid.ZERO_UUID, -1)));

        // shareFetchData is not empty, but the maxBytes of topic partition is 0, which means this is added only for acknowledgements.
        // New context should be created successfully
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData2 = Collections.singletonMap(new TopicIdPartition(topicId, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(topicId, 0));
        shareFetchContext = sharePartitionManager.newContext("grp", shareFetchData2, Collections.emptyList(), newReqMetadata);
        assertEquals(FinalContext.class, shareFetchContext.getClass());
    }

    @Test
    public void testNewContextReturnsFinalContextForEmptyTopicPartitionsAndFinalEpoch() {
        Time time = new MockTime();
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        String groupId = "grp";
        // Verify that final epoch requests get a FinalContext
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(FinalContext.class, context1.getClass());
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


        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2);
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareFetchMetadata(memberId4, 1)));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        // Close the subsequent share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(0, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
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
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session1context = sharePartitionManager.newContext(groupId, session1req, EMPTY_PART_LIST, reqMetadata1);
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

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session2context = sharePartitionManager.newContext(groupId, session2req, EMPTY_PART_LIST, reqMetadata2);
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
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
        assertEquals(session1context2.getClass(), ShareSessionContext.class);

        // total sleep time will now be large enough that share session 1 will be evicted if not correctly touched
        time.sleep(501);

        // create one final share session to test that the least recently used entry is evicted
        // the second share session should be evicted because the first share session was incrementally fetched
        // more recently than the second session was created
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session3req = new LinkedHashMap<>();
        session3req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session3req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        ShareFetchMetadata reqMetadata3 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session3context = sharePartitionManager.newContext(groupId, session3req, EMPTY_PART_LIST, reqMetadata3);

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
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1);
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
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
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
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1);
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
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
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

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo, new ShareFetchRequest.SharePartitionData(foo.topicId(), 100));
        reqData1.put(bar, new ShareFetchRequest.SharePartitionData(bar.topicId(), 100));


        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1);
        assertEquals(ShareSessionContext.class, context1.getClass());
        assertPartitionsPresent((ShareSessionContext) context1, Arrays.asList(foo, bar));

        mockUpdateAndGenerateResponseData(context1, groupId, reqMetadata1.memberId());

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(foo),
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));

        // So foo is removed but not the others.
        assertPartitionsPresent((ShareSessionContext) context2, Collections.singletonList(bar));

        mockUpdateAndGenerateResponseData(context2, groupId, reqMetadata1.memberId());

        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(bar),
                new ShareFetchMetadata(reqMetadata1.memberId(), 2));
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

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1);

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
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));

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


        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2);
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareFetchMetadata(Uuid.randomUuid(), 1)));

        // Continue the first share session we created.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(ShareSessionContext.class, context5.getClass());
        assertTrue(((ShareSessionContext) context5).isSubsequent());

        assertErroneousAndValidTopicIdPartitions(context5.getErroneousAndValidTopicIdPartitions(), Collections.singletonList(tpNull1), Arrays.asList(tp0, tp1));

        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = Collections.singletonMap(
                tpNull2, new ShareFetchRequest.SharePartitionData(tpNull2.topicId(), 100));
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        // Check for throttled response
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        assertErroneousAndValidTopicIdPartitions(context7.getErroneousAndValidTopicIdPartitions(), Arrays.asList(tpNull1, tpNull2), Arrays.asList(tp0, tp1));

        // Close the subsequent share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(0, cache.size());


        assertErroneousAndValidTopicIdPartitions(context8.getErroneousAndValidTopicIdPartitions(), Collections.emptyList(), Collections.emptyList());
        // Check for throttled response
        ShareFetchResponse resp8 = context8.throttleResponse(100);
        assertEquals(Errors.NONE, resp8.error());
        assertEquals(100, resp8.throttleTimeMs());
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

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2);
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST,
                new ShareFetchMetadata(memberId4, 1)));

        // Continue the first share session we created.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = Collections.singletonMap(
                tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));

        int respSize7 = context7.responseSize(respData2, version);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + new ShareFetchResponseData().size(objectSerializationCache, version), respSize7);

        // Close the subsequent share session.
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(FinalContext.class, context8.getClass());
        assertEquals(0, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize8 = context8.responseSize(respData8, version);
        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
        // We add 4 here in response to 4 being added in sizeOf() method in ShareFetchResponse class.
        assertEquals(4 + resp8.data().size(objectSerializationCache, version), respSize8);
    }

    @Test
    public void testCachedTopicPartitionsForInvalidShareSession() {
        ShareSessionCache cache = new ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();

        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.cachedTopicIdPartitionsInShareSession("grp", Uuid.randomUuid()));
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

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(memberId1, ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, EMPTY_PART_LIST, reqMetadata1);
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

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(memberId2, ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, EMPTY_PART_LIST, reqMetadata2);
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
                new ShareFetchMetadata(shareSessionKey1.memberId(), 1));
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
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(ShareSessionContext.class, context4.getClass());
        assertTrue(((ShareSessionContext) context4).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData4 = new LinkedHashMap<>();
        respData4.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        ShareFetchResponse resp4 = context4.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData4);
        assertEquals(Errors.NONE, resp4.error());

        assertEquals(Collections.singletonList(tp3), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Close the first share session.
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), EMPTY_PART_LIST,
                new ShareFetchMetadata(reqMetadata1.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(FinalContext.class, context5.getClass());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData5);
        assertEquals(Errors.NONE, resp5.error());

        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1));

        // Continue the second share session .
        ShareFetchContext context6 = sharePartitionManager.newContext(groupId, Collections.emptyMap(), Collections.singletonList(tp3),
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
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

    private void assertErroneousAndValidTopicIdPartitions(ErroneousAndValidPartitionData erroneousAndValidPartitionData,
                                                          List<TopicIdPartition> expectedErroneous, List<TopicIdPartition> expectedValid) {
        Set<TopicIdPartition> expectedErroneousSet = new HashSet<>(expectedErroneous);
        Set<TopicIdPartition> expectedValidSet = new HashSet<>(expectedValid);
        Set<TopicIdPartition> actualErroneousPartitions = new HashSet<>();
        Set<TopicIdPartition> actualValidPartitions = new HashSet<>();
        erroneousAndValidPartitionData.erroneous().forEach(topicIdPartitionPartitionDataTuple2 ->
                actualErroneousPartitions.add(topicIdPartitionPartitionDataTuple2._1));
        erroneousAndValidPartitionData.validTopicIdPartitions().forEach(topicIdPartitionPartitionDataTuple2 ->
                actualValidPartitions.add(topicIdPartitionPartitionDataTuple2._1));
        assertEquals(expectedErroneousSet, actualErroneousPartitions);
        assertEquals(expectedValidSet, actualValidPartitions);
    }

    private static class SharePartitionManagerBuilder {
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private Time time = new MockTime();
        private ShareSessionCache cache = new ShareSessionCache(10, 1000);
        private Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        private Persister persister = NoOpShareStatePersister.getInstance();

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

        public static SharePartitionManagerBuilder builder() {
            return new SharePartitionManagerBuilder();
        }

        public SharePartitionManager build() {
            return new SharePartitionManager(replicaManager, time, cache, partitionCacheMap, RECORD_LOCK_DURATION_MS, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES, persister);
        }
    }
}
