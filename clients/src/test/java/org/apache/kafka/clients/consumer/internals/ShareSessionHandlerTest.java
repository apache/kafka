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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.ShareRequestMetadata.INITIAL_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for ShareSessionHandler.
 */
@Timeout(120)
public class ShareSessionHandlerTest {
    private static final LogContext LOG_CONTEXT = new LogContext("[ShareSessionHandler]=");
    private final FetchConfig fetchConfig = new FetchConfig(
            ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            true,
            ConsumerConfig.DEFAULT_CLIENT_RACK,
            IsolationLevel.READ_UNCOMMITTED);

    private static LinkedHashMap<TopicPartition, TopicIdPartition> reqMap(TopicIdPartition... entries) {
        LinkedHashMap<TopicPartition, TopicIdPartition> map = new LinkedHashMap<>();
        for (TopicIdPartition entry : entries) {
            map.put(entry.topicPartition(), entry);
        }
        return map;
    }

    private static ArrayList<TopicIdPartition> reqFetchList(ShareFetchRequestData requestData, Map<Uuid, String> topicNames) {
        ArrayList<TopicIdPartition> tips = new ArrayList<>();
        requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> tips.add(new TopicIdPartition(topic.topicId(), partition.partitionIndex(),
                topicNames.get(topic.topicId())))));
        return tips;
    }

    private static ArrayList<TopicIdPartition> reqForgetList(ShareFetchRequestData requestData, Map<Uuid, String> topicNames) {
        ArrayList<TopicIdPartition> tips = new ArrayList<>();
        requestData.forgottenTopicsData().forEach(topic -> topic.partitions().forEach(partition -> tips.add(new TopicIdPartition(topic.topicId(), partition, topicNames.get(topic.topicId())))));
        return tips;
    }

    private static void assertMapEquals(Map<TopicPartition, TopicIdPartition> expected,
                                        Map<TopicPartition, TopicIdPartition> actual) {
        Iterator<Map.Entry<TopicPartition, TopicIdPartition>> expectedIter =
                expected.entrySet().iterator();
        Iterator<Map.Entry<TopicPartition, TopicIdPartition>> actualIter =
                actual.entrySet().iterator();
        int i = 1;
        while (expectedIter.hasNext()) {
            Map.Entry<TopicPartition, TopicIdPartition> expectedEntry = expectedIter.next();
            if (!actualIter.hasNext()) {
                fail("Element " + i + " not found.");
            }
            Map.Entry<TopicPartition, TopicIdPartition> actualEntry = actualIter.next();
            assertEquals(expectedEntry.getKey(), actualEntry.getKey(), "Element " + i +
                    " had a different TopicPartition than expected.");
            assertEquals(expectedEntry.getValue(), actualEntry.getValue(), "Element " + i +
                    " had different PartitionData than expected.");
            i++;
        }
        if (actualIter.hasNext()) {
            fail("Unexpected element " + i + " found.");
        }
    }

    @SafeVarargs
    private static void assertMapsEqual(Map<TopicPartition, TopicIdPartition> expected,
                                        Map<TopicPartition, TopicIdPartition>... actuals) {
        for (Map<TopicPartition, TopicIdPartition> actual : actuals) {
            assertMapEquals(expected, actual);
        }
    }

    private static void assertListEquals(List<TopicIdPartition> expected, List<TopicIdPartition> actual) {
        for (TopicIdPartition expectedPart : expected) {
            if (!actual.contains(expectedPart)) {
                fail("Failed to find expected partition " + expectedPart);
            }
        }
        for (TopicIdPartition actualPart : actual) {
            if (!expected.contains(actualPart)) {
                fail("Found unexpected partition " + actualPart);
            }
        }
    }

    private static final class RespEntry {
        final TopicIdPartition part;
        final ShareFetchResponseData.PartitionData data;

        RespEntry(String topic, int partition, Uuid topicId) {
            this.part = new TopicIdPartition(topicId, partition, topic);
            this.data = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(partition);
        }
    }

    private static List<ShareFetchResponseData.ShareFetchableTopicResponse> respList(RespEntry... entries) {
        HashMap<TopicIdPartition, ShareFetchResponseData.ShareFetchableTopicResponse> map = new HashMap<>();
        for (RespEntry entry : entries) {
            ShareFetchResponseData.ShareFetchableTopicResponse response = map.computeIfAbsent(entry.part, topicIdPartition ->
                    new ShareFetchResponseData.ShareFetchableTopicResponse().setTopicId(topicIdPartition.topicId()));
            response.partitions().add(new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(entry.part.partition()));
        }
        return new ArrayList<>(map.values());
    }

    @Test
    public void testShareSession() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));
        assertEquals(memberId.toString(), requestData1.memberId());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, fooId),
                                new RespEntry("foo", 1, fooId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Test a fetch request which adds one partition
        Uuid barId = addTopicId(topicIds, topicNames, "bar");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        handler.addPartitionToFetch(bar0, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend2 = new ArrayList<>();
        expectedToSend2.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend2, reqFetchList(requestData2, topicNames));

        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 1, fooId))));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        // A top-level error code will reset the session epoch
        ShareFetchResponse resp3 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.INVALID_SHARE_SESSION_EPOCH.code()));
        handler.handleResponse(resp3, ApiKeys.SHARE_FETCH.latestVersion(true));

        ShareFetchRequestData requestData4 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertEquals(requestData2.memberId(), requestData4.memberId());
        assertEquals(INITIAL_EPOCH, requestData4.shareSessionEpoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend4 = new ArrayList<>();
        expectedToSend4.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend4.add(new TopicIdPartition(fooId, 1, "foo"));
        expectedToSend4.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend4, reqFetchList(requestData4, topicNames));
    }

    @Test
    public void testPartitionRemoval() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicIds, topicNames, "foo");
        Uuid barId = addTopicId(topicIds, topicNames, "bar");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        handler.addPartitionToFetch(bar0, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertMapsEqual(reqMap(
                        new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        expectedToSend1.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));
        assertEquals(memberId.toString(), requestData1.memberId());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, fooId),
                                new RespEntry("foo", 1, fooId),
                                new RespEntry("bar", 0, barId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Test a fetch request which removes two partitions
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertEquals(memberId.toString(), requestData2.memberId());
        assertEquals(1, requestData2.shareSessionEpoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 1, "foo")),
                handler.sessionPartitionMap());
        assertTrue(requestData2.topics().isEmpty());
        ArrayList<TopicIdPartition> expectedToForget2 = new ArrayList<>();
        expectedToForget2.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToForget2.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToForget2, reqForgetList(requestData2, topicNames));

        // A top-level error code will reset the session epoch
        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.INVALID_SHARE_SESSION_EPOCH.code()));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData3 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertEquals(memberId.toString(), requestData3.memberId());
        assertEquals(INITIAL_EPOCH, requestData3.shareSessionEpoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 1, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend3 = new ArrayList<>();
        expectedToSend3.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend3, reqFetchList(requestData3, topicNames));
    }

    @Test
    public void testTopicIdReplaced() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId1 = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition tp = new TopicIdPartition(topicId1, 0, "foo");
        handler.addPartitionToFetch(tp, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId1, 0, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId1, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId1))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Try to add a new topic ID
        Uuid topicId2 = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition tp2 = new TopicIdPartition(topicId2, 0, "foo");
        // Use the same data besides the topic ID
        handler.addPartitionToFetch(tp2, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();

        // If we started with an ID, only a new ID will count towards replaced.
        // The old topic ID partition should be forgotten, and the new one should be fetched.
        assertEquals(Collections.singletonList(tp), reqForgetList(requestData2, topicNames));
        assertMapsEqual(reqMap(new TopicIdPartition(topicId2, 0, "foo")),
                handler.sessionPartitionMap());
        assertListEquals(Collections.singletonList(tp2), reqFetchList(requestData2, topicNames));

        // Should have the same session ID, and next epoch and can use topic IDs if it ended with topic IDs.
        assertEquals(memberId.toString(), requestData2.memberId(), "Did not use same session");
        assertEquals(1, requestData2.shareSessionEpoch(), "Did not have correct epoch");
    }

    @Test
    public void testForgottenPartitions() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        // We want to test when all topics are removed from the session
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(topicId, 0, "foo");
        handler.addPartitionToFetch(foo0, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertMapsEqual(reqMap(foo0), handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Remove the topic from the session
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertEquals(Collections.singletonList(foo0), reqForgetList(requestData2, topicNames));

        // Should have the same session ID, next epoch, and same ID usage
        assertEquals(memberId.toString(), requestData2.memberId(), "Did not use same session");
        assertEquals(1, requestData2.shareSessionEpoch(), "Did not have correct epoch");
    }

    @Test
    public void testAddNewIdAfterTopicRemovedFromSession() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId = addTopicId(topicIds, topicNames, "foo");
        handler.addPartitionToFetch(new TopicIdPartition(topicId, 0, "foo"), null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId, 0, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Remove the partition from the session
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();
        assertTrue(handler.sessionPartitionMap().isEmpty());
        assertTrue(requestData2.topics().isEmpty());
        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList()));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        // After the topic is removed, add a recreated topic with a new ID
        Uuid topicId2 = addTopicId(topicIds, topicNames, "foo");
        handler.addPartitionToFetch(new TopicIdPartition(topicId2, 0, "foo"), null);
        ShareFetchRequestData requestData3 = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();

        // Should have the same session ID and epoch 2.
        assertEquals(memberId.toString(), requestData3.memberId(), "Did not use same session");
        assertEquals(2, requestData3.shareSessionEpoch(), "Did not have the correct session epoch");
    }

    @Test
    public void testNextAcknowledgementsClearedOnInvalidRequest() {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);

        handler.addPartitionToFetch(foo0, acknowledgements);

        // As we start with a ShareAcknowledge on epoch 0, we expect a null response.
        assertNull(handler.newShareAcknowledgeBuilder(groupId, fetchConfig));

        // Attempt a new ShareFetch
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData = handler.newShareFetchBuilder(groupId, fetchConfig).build().data();

        // We should have cleared the unsent acknowledgements before this ShareFetch.
        assertEquals(0, requestData.topics().get(0).partitions().get(0).acknowledgementBatches().size());

        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData, topicNames));
        assertEquals(memberId.toString(), requestData.memberId());
    }

    private Uuid addTopicId(Map<String, Uuid> topicIds, Map<Uuid, String> topicNames, String name) {
        // If the same topic name is added more than once, the latest mapping will be in the
        // topicIds, but all mappings will be in topicNames. This is needed in the replace tests.
        Uuid id = Uuid.randomUuid();
        topicNames.put(id, name);
        return id;
    }
}
