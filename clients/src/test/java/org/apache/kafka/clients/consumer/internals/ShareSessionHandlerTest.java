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
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ShareRequestMetadata.INITIAL_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for ShareSessionHandler.
 */
@Timeout(120)
public class ShareSessionHandlerTest {
    private static final LogContext LOG_CONTEXT = new LogContext("[ShareSessionHandler]=");
    private static final ShareFetchConfig DEFAULT_SHARE_FETCH_CONFIG = new ShareFetchConfig(
            ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            true,
            ConsumerConfig.DEFAULT_CLIENT_RACK,
            IsolationLevel.READ_UNCOMMITTED,
            ShareAcquireMode.BATCH_OPTIMIZED);
    private static final ShareFetchConfig SHARE_FETCH_CONFIG_RECORD_LIMIT = new ShareFetchConfig(
            ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            true,
            ConsumerConfig.DEFAULT_CLIENT_RACK,
            IsolationLevel.READ_UNCOMMITTED,
            ShareAcquireMode.RECORD_LIMIT);

    private static Stream<ShareFetchConfig> shareFetchConfigProvider() {
        return Stream.of(DEFAULT_SHARE_FETCH_CONFIG, SHARE_FETCH_CONFIG_RECORD_LIMIT);
    }

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

    private static LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> buildResponseData(RespEntry... entries) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> topicIdPartitionToPartition = new LinkedHashMap<>();
        for (RespEntry entry : entries) {
            ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(entry.part.partition());
            topicIdPartitionToPartition.put(entry.part, partitionData);
        }
        return topicIdPartitionToPartition;
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"INVALID_SHARE_SESSION_EPOCH", "SHARE_SESSION_NOT_FOUND", "SHARE_SESSION_LIMIT_REACHED"})
    public void testShareSession(Errors error) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, DEFAULT_SHARE_FETCH_CONFIG, false).build().data();
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));
        assertEquals(memberId.toString(), requestData1.memberId());

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, fooId), new RespEntry("foo", 1, fooId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Test a fetch request which adds one partition
        Uuid barId = addTopicId(topicNames, "bar");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        handler.addPartitionToFetch(bar0, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, DEFAULT_SHARE_FETCH_CONFIG, false).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend2 = new ArrayList<>();
        expectedToSend2.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend2, reqFetchList(requestData2, topicNames));

        ShareFetchResponse resp2 = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 1, fooId)),
            List.of(),
            0);
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion());

        // A top-level error code will reset the session epoch
        ShareFetchResponse resp3 = ShareFetchResponse.of(error, 0, new LinkedHashMap<>(), List.of(), 0);
        handler.handleResponse(resp3, ApiKeys.SHARE_FETCH.latestVersion());

        ShareFetchRequestData requestData4 = handler.newShareFetchBuilder(groupId, DEFAULT_SHARE_FETCH_CONFIG, false).build().data();
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

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testPartitionRemoval(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicNames, "foo");
        Uuid barId = addTopicId(topicNames, "bar");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        handler.addPartitionToFetch(foo0, null);
        handler.addPartitionToFetch(foo1, null);
        handler.addPartitionToFetch(bar0, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
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

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(
                new RespEntry("foo", 0, fooId),
                new RespEntry("foo", 1, fooId),
                new RespEntry("bar", 0, barId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Test a fetch request which removes two partitions
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
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
        ShareFetchResponse resp2 = ShareFetchResponse.of(Errors.INVALID_SHARE_SESSION_EPOCH, 0, new LinkedHashMap<>(), List.of(), 0);
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion());

        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData3 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertEquals(memberId.toString(), requestData3.memberId());
        assertEquals(INITIAL_EPOCH, requestData3.shareSessionEpoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 1, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend3 = new ArrayList<>();
        expectedToSend3.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend3, reqFetchList(requestData3, topicNames));
    }

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testTopicIdReplaced(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId1 = addTopicId(topicNames, "foo");
        TopicIdPartition tp = new TopicIdPartition(topicId1, 0, "foo");
        handler.addPartitionToFetch(tp, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId1, 0, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId1, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, topicId1)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Try to add a new topic ID
        Uuid topicId2 = addTopicId(topicNames, "foo");
        TopicIdPartition tp2 = new TopicIdPartition(topicId2, 0, "foo");
        // Use the same data besides the topic ID
        handler.addPartitionToFetch(tp2, null);
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();

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

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testPartitionForgottenOnAcknowledgeOnly(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        // We want to test when all topics are removed from the session
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId = addTopicId(topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(topicId, 0, "foo");
        handler.addPartitionToFetch(foo0, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertMapsEqual(reqMap(foo0), handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, topicId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Remove the topic from the session by setting acknowledgements only - this is not asking to fetch records
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        handler.addPartitionToAcknowledgeOnly(foo0, Acknowledgements.empty());
        assertEquals(Collections.singletonList(foo0), reqForgetList(requestData2, topicNames));

        // Should have the same session ID, next epoch, and same ID usage
        assertEquals(memberId.toString(), requestData2.memberId(), "Did not use same session");
        assertEquals(1, requestData2.shareSessionEpoch(), "Did not have correct epoch");
    }

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testForgottenPartitions(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        // We want to test when all topics are removed from the session
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId = addTopicId(topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(topicId, 0, "foo");
        handler.addPartitionToFetch(foo0, null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertMapsEqual(reqMap(foo0), handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, topicId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Remove the topic from the session
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertEquals(Collections.singletonList(foo0), reqForgetList(requestData2, topicNames));

        // Should have the same session ID, next epoch, and same ID usage
        assertEquals(memberId.toString(), requestData2.memberId(), "Did not use same session");
        assertEquals(1, requestData2.shareSessionEpoch(), "Did not have correct epoch");
    }

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testAddNewIdAfterTopicRemovedFromSession(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid topicId = addTopicId(topicNames, "foo");
        handler.addPartitionToFetch(new TopicIdPartition(topicId, 0, "foo"), null);
        ShareFetchRequestData requestData1 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId, 0, "foo")),
                handler.sessionPartitionMap());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData1, topicNames));

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, topicId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // Remove the partition from the session
        ShareFetchRequestData requestData2 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();
        assertTrue(handler.sessionPartitionMap().isEmpty());
        assertTrue(requestData2.topics().isEmpty());
        ShareFetchResponse resp2 = ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0);
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion());

        // After the topic is removed, add a recreated topic with a new ID
        Uuid topicId2 = addTopicId(topicNames, "foo");
        handler.addPartitionToFetch(new TopicIdPartition(topicId2, 0, "foo"), null);
        ShareFetchRequestData requestData3 = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();

        // Should have the same session ID and epoch 2.
        assertEquals(memberId.toString(), requestData3.memberId(), "Did not use same session");
        assertEquals(2, requestData3.shareSessionEpoch(), "Did not have the correct session epoch");
    }

    @ParameterizedTest
    @MethodSource("shareFetchConfigProvider")
    public void testNextAcknowledgementsClearedOnInvalidRequest(ShareFetchConfig shareFetchConfig) {
        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);

        handler.addPartitionToFetch(foo0, acknowledgements);

        // As we start with a ShareAcknowledge on epoch 0, we expect a null response.
        assertNull(handler.newShareAcknowledgeBuilder(groupId, shareFetchConfig));

        // Attempt a new ShareFetch
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        handler.addPartitionToFetch(foo1, null);
        ShareFetchRequestData requestData = handler.newShareFetchBuilder(groupId, shareFetchConfig, false).build().data();

        // We should have cleared the unsent acknowledgements before this ShareFetch.
        assertEquals(0, requestData.topics().stream().findFirst().get().partitions().stream().findFirst().get().acknowledgementBatches().size());

        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend1, reqFetchList(requestData, topicNames));
        assertEquals(memberId.toString(), requestData.memberId());
    }

    @Test
    public void testCanSkipIfRequestEmpty() {
        ShareFetchConfig shareFetchConfig = SHARE_FETCH_CONFIG_RECORD_LIMIT;

        String groupId = "G1";
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = addTopicId(topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);

        // The request cannot be skipped when a topic-partition is added to the share session.
        handler.addPartitionToFetch(foo0, null);
        ShareFetchRequest.Builder builder = handler.newShareFetchBuilder(groupId, shareFetchConfig, true);
        assertNotNull(builder);

        ShareFetchResponse resp = ShareFetchResponse.of(Errors.NONE,
            0,
            buildResponseData(new RespEntry("foo", 0, fooId)),
            List.of(),
            0);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // The request can be skipped when the same topic-partition is already in the share session.
        handler.addPartitionToFetch(foo0, null);
        builder = handler.newShareFetchBuilder(groupId, shareFetchConfig, true);
        assertNull(builder);

        // The request cannot be skipped when there are acknowledgements.
        handler.addPartitionToFetch(foo0, acknowledgements);
        builder = handler.newShareFetchBuilder(groupId, shareFetchConfig, true);
        assertNotNull(builder);
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion());

        // The request cannot be skipped when the topic-partition is removed from the share session.
        builder = handler.newShareFetchBuilder(groupId, shareFetchConfig, true);
        assertNotNull(builder);
        handler.handleResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0), ApiKeys.SHARE_FETCH.latestVersion());

        // The request can be skipped when the share session is empty.
        builder = handler.newShareFetchBuilder(groupId, shareFetchConfig, true);
        assertNull(builder);
    }

    private Uuid addTopicId(Map<Uuid, String> topicNames, String name) {
        Uuid id = Uuid.randomUuid();
        topicNames.put(id, name);
        return id;
    }
}
