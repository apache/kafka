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
package org.apache.kafka.clients;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.TreeSet;

import static org.apache.kafka.common.requests.FetchMetadata.INITIAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for FetchSessionHandler.
 */
@Timeout(120)
public class FetchSessionHandlerTest {
    private static final LogContext LOG_CONTEXT = new LogContext("[FetchSessionHandler]=");

    /**
     * Create a set of TopicPartitions.  We use a TreeSet, in order to get a deterministic
     * ordering for test purposes.
     */
    private static Set<TopicPartition> toSet(TopicPartition... arr) {
        TreeSet<TopicPartition> set = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
        set.addAll(Arrays.asList(arr));
        return set;
    }

    @Test
    public void testFindMissing() {
        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition foo1 = new TopicPartition("foo", 1);
        TopicPartition bar0 = new TopicPartition("bar", 0);
        TopicPartition bar1 = new TopicPartition("bar", 1);
        TopicPartition baz0 = new TopicPartition("baz", 0);
        TopicPartition baz1 = new TopicPartition("baz", 1);
        assertEquals(toSet(), FetchSessionHandler.findMissing(toSet(foo0), toSet(foo0)));
        assertEquals(toSet(foo0), FetchSessionHandler.findMissing(toSet(foo0), toSet(foo1)));
        assertEquals(toSet(foo0, foo1),
            FetchSessionHandler.findMissing(toSet(foo0, foo1), toSet(baz0)));
        assertEquals(toSet(bar1, foo0, foo1),
            FetchSessionHandler.findMissing(toSet(foo0, foo1, bar0, bar1),
                toSet(bar0, baz0, baz1)));
        assertEquals(toSet(),
            FetchSessionHandler.findMissing(toSet(foo0, foo1, bar0, bar1, baz1),
                toSet(foo0, foo1, bar0, bar1, baz0, baz1)));
    }

    private static final class ReqEntry {
        final TopicPartition part;
        final FetchRequest.PartitionData data;

        ReqEntry(String topic, Uuid topicId, int partition, long fetchOffset, long logStartOffset, int maxBytes) {
            this.part = new TopicPartition(topic, partition);
            this.data = new FetchRequest.PartitionData(topicId, fetchOffset, logStartOffset, maxBytes, Optional.empty());
        }
    }

    private static LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqMap(ReqEntry... entries) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> map = new LinkedHashMap<>();
        for (ReqEntry entry : entries) {
            map.put(entry.part, entry.data);
        }
        return map;
    }

    private static void assertMapEquals(Map<TopicPartition, FetchRequest.PartitionData> expected,
                                        Map<TopicPartition, FetchRequest.PartitionData> actual) {
        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> expectedIter =
            expected.entrySet().iterator();
        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> actualIter =
            actual.entrySet().iterator();
        int i = 1;
        while (expectedIter.hasNext()) {
            Map.Entry<TopicPartition, FetchRequest.PartitionData> expectedEntry = expectedIter.next();
            if (!actualIter.hasNext()) {
                fail("Element " + i + " not found.");
            }
            Map.Entry<TopicPartition, FetchRequest.PartitionData> actuaLEntry = actualIter.next();
            assertEquals(expectedEntry.getKey(), actuaLEntry.getKey(), "Element " + i +
                " had a different TopicPartition than expected.");
            assertEquals(expectedEntry.getValue(), actuaLEntry.getValue(), "Element " + i +
                " had different PartitionData than expected.");
            i++;
        }
        if (actualIter.hasNext()) {
            fail("Unexpected element " + i + " found.");
        }
    }

    @SafeVarargs
    private static void assertMapsEqual(Map<TopicPartition, FetchRequest.PartitionData> expected,
                                        Map<TopicPartition, FetchRequest.PartitionData>... actuals) {
        for (Map<TopicPartition, FetchRequest.PartitionData> actual : actuals) {
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
        final FetchResponseData.PartitionData data;

        RespEntry(String topic, int partition, Uuid topicId, long highWatermark, long lastStableOffset) {
            this.part = new TopicIdPartition(topicId, new TopicPartition(topic, partition));

            this.data = new FetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setHighWatermark(highWatermark)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(0);
        }

        RespEntry(String topic, int partition, Uuid topicId, Errors error) {
            this.part = new TopicIdPartition(topicId, new TopicPartition(topic, partition));

            this.data = new FetchResponseData.PartitionData()
                    .setPartitionIndex(partition)
                    .setErrorCode(error.code())
                    .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK);
        }
    }

    private static LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> respMap(RespEntry... entries) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> map = new LinkedHashMap<>();
        for (RespEntry entry : entries) {
            map.put(entry.part, entry.data);
        }
        return map;
    }

    /**
     * Test the handling of SESSIONLESS responses.
     * Pre-KIP-227 brokers always supply this kind of response.
     */
    @Test
    public void testSessionless() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        List<Short> versions = Arrays.asList((short) 12, ApiKeys.FETCH.latestVersion());
        versions.forEach(version -> {
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            FetchSessionHandler.Builder builder = handler.newBuilder();
            addTopicId(topicIds, topicNames, "foo", version);
            Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
            builder.add(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder.add(new TopicPartition("foo", 1),
                    new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
            FetchSessionHandler.FetchRequestData data = builder.build();
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200),
                    new ReqEntry("foo", fooId, 1, 10, 110, 210)),
                    data.toSend(), data.sessionPartitions());
            assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
            assertEquals(INITIAL_EPOCH, data.metadata().epoch());

            FetchResponse resp = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
                respMap(new RespEntry("foo", 0, fooId, 0, 0),
                        new RespEntry("foo", 1, fooId, 0, 0)));
            handler.handleResponse(resp, version);

            FetchSessionHandler.Builder builder2 = handler.newBuilder();
            builder2.add(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data2 = builder2.build();
            assertEquals(INVALID_SESSION_ID, data2.metadata().sessionId());
            assertEquals(INITIAL_EPOCH, data2.metadata().epoch());
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200)),
                    data2.toSend(), data2.sessionPartitions());
        });
    }

    /**
     * Test handling an incremental fetch session.
     */
    @Test
    public void testIncrementals() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        List<Short> versions = Arrays.asList((short) 12, ApiKeys.FETCH.latestVersion());
        versions.forEach(version -> {
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            FetchSessionHandler.Builder builder = handler.newBuilder();
            addTopicId(topicIds, topicNames, "foo", version);
            Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
            TopicPartition foo0 = new TopicPartition("foo", 0);
            TopicPartition foo1 = new TopicPartition("foo", 1);
            builder.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder.add(foo1, new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
            FetchSessionHandler.FetchRequestData data = builder.build();
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200),
                    new ReqEntry("foo", fooId, 1, 10, 110, 210)),
                    data.toSend(), data.sessionPartitions());
            assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
            assertEquals(INITIAL_EPOCH, data.metadata().epoch());

            FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 0, fooId, 10, 20),
                        new RespEntry("foo", 1, fooId, 10, 20)));
            handler.handleResponse(resp, version);

            // Test an incremental fetch request which adds one partition and modifies another.
            FetchSessionHandler.Builder builder2 = handler.newBuilder();
            addTopicId(topicIds, topicNames, "bar", version);
            Uuid barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID);
            TopicPartition bar0 = new TopicPartition("bar", 0);
            builder2.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder2.add(foo1, new FetchRequest.PartitionData(fooId, 10, 120, 210, Optional.empty()));
            builder2.add(bar0, new FetchRequest.PartitionData(barId, 20, 200, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data2 = builder2.build();
            assertFalse(data2.metadata().isFull());
            assertMapEquals(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200),
                    new ReqEntry("foo", fooId, 1, 10, 120, 210),
                    new ReqEntry("bar", barId, 0, 20, 200, 200)),
                    data2.sessionPartitions());
            assertMapEquals(reqMap(new ReqEntry("bar", barId, 0, 20, 200, 200),
                    new ReqEntry("foo", fooId, 1, 10, 120, 210)),
                    data2.toSend());

            FetchResponse resp2 = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 1, fooId, 20, 20)));
            handler.handleResponse(resp2, version);

            // Skip building a new request.  Test that handling an invalid fetch session epoch response results
            // in a request which closes the session.
            FetchResponse resp3 = FetchResponse.of(Errors.INVALID_FETCH_SESSION_EPOCH, 0, INVALID_SESSION_ID,
                respMap());
            handler.handleResponse(resp3, version);

            FetchSessionHandler.Builder builder4 = handler.newBuilder();
            builder4.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder4.add(foo1, new FetchRequest.PartitionData(fooId, 10, 120, 210, Optional.empty()));
            builder4.add(bar0, new FetchRequest.PartitionData(barId, 20, 200, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data4 = builder4.build();
            assertTrue(data4.metadata().isFull());
            assertEquals(data2.metadata().sessionId(), data4.metadata().sessionId());
            assertEquals(INITIAL_EPOCH, data4.metadata().epoch());
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200),
                    new ReqEntry("foo", fooId, 1, 10, 120, 210),
                    new ReqEntry("bar", barId, 0, 20, 200, 200)),
                    data4.sessionPartitions(), data4.toSend());
        });
    }

    /**
     * Test that calling FetchSessionHandler#Builder#build twice fails.
     */
    @Test
    public void testDoubleBuild() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0),
            new FetchRequest.PartitionData(Uuid.randomUuid(), 0, 100, 200, Optional.empty()));
        builder.build();
        assertThrows(Throwable.class, builder::build, "Expected calling build twice to fail.");
    }

    @Test
    public void testIncrementalPartitionRemoval() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        List<Short> versions = Arrays.asList((short) 12, ApiKeys.FETCH.latestVersion());
        versions.forEach(version -> {
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            FetchSessionHandler.Builder builder = handler.newBuilder();
            addTopicId(topicIds, topicNames, "foo", version);
            addTopicId(topicIds, topicNames, "bar", version);
            Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
            Uuid barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID);
            TopicPartition foo0 = new TopicPartition("foo", 0);
            TopicPartition foo1 = new TopicPartition("foo", 1);
            TopicPartition bar0 = new TopicPartition("bar", 0);
            builder.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder.add(foo1, new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
            builder.add(bar0, new FetchRequest.PartitionData(barId, 20, 120, 220, Optional.empty()));
            FetchSessionHandler.FetchRequestData data = builder.build();
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200),
                    new ReqEntry("foo", fooId, 1, 10, 110, 210),
                    new ReqEntry("bar", barId, 0, 20, 120, 220)),
                    data.toSend(), data.sessionPartitions());
            assertTrue(data.metadata().isFull());

            FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 0, fooId, 10, 20),
                        new RespEntry("foo", 1, fooId, 10, 20),
                        new RespEntry("bar", 0, barId, 10, 20)));
            handler.handleResponse(resp, version);

            // Test an incremental fetch request which removes two partitions.
            FetchSessionHandler.Builder builder2 = handler.newBuilder();
            builder2.add(foo1, new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
            FetchSessionHandler.FetchRequestData data2 = builder2.build();
            assertFalse(data2.metadata().isFull());
            assertEquals(123, data2.metadata().sessionId());
            assertEquals(1, data2.metadata().epoch());
            assertMapEquals(reqMap(new ReqEntry("foo", fooId, 1, 10, 110, 210)),
                    data2.sessionPartitions());
            assertMapEquals(reqMap(), data2.toSend());
            ArrayList<TopicIdPartition> expectedToForget2 = new ArrayList<>();
            expectedToForget2.add(new TopicIdPartition(fooId, foo0));
            expectedToForget2.add(new TopicIdPartition(barId, bar0));
            assertListEquals(expectedToForget2, data2.toForget());

            // A FETCH_SESSION_ID_NOT_FOUND response triggers us to close the session.
            // The next request is a session establishing FULL request.
            FetchResponse resp2 = FetchResponse.of(Errors.FETCH_SESSION_ID_NOT_FOUND, 0, INVALID_SESSION_ID,
                respMap());
            handler.handleResponse(resp2, version);

            FetchSessionHandler.Builder builder3 = handler.newBuilder();
            builder3.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data3 = builder3.build();
            assertTrue(data3.metadata().isFull());
            assertEquals(INVALID_SESSION_ID, data3.metadata().sessionId());
            assertEquals(INITIAL_EPOCH, data3.metadata().epoch());
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200)),
                    data3.sessionPartitions(), data3.toSend());
        });
    }

    @Test
    public void testTopicIdUsageGrantedOnIdUpgrade() {
        // We want to test adding a topic ID to an existing partition and a new partition in the incremental request.
        // 0 is the existing partition and 1 is the new one.
        List<Integer> partitions = Arrays.asList(0, 1);
        partitions.forEach(partition -> {
            String testType = partition == 0 ? "updating a partition" : "adding a new partition";
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            FetchSessionHandler.Builder builder = handler.newBuilder();
            builder.add(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 100, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data = builder.build();
            assertMapsEqual(reqMap(new ReqEntry("foo", Uuid.ZERO_UUID, 0, 0, 100, 200)),
                    data.toSend(), data.sessionPartitions());
            assertTrue(data.metadata().isFull());
            assertFalse(data.canUseTopicIds());

            FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                    respMap(new RespEntry("foo", 0, Uuid.ZERO_UUID, 10, 20)));
            handler.handleResponse(resp, (short) 12);

            // Try to add a topic ID to an already existing topic partition (0) or a new partition (1) in the session.
            Uuid topicId = Uuid.randomUuid();
            FetchSessionHandler.Builder builder2 = handler.newBuilder();
            builder2.add(new TopicPartition("foo", partition),
                    new FetchRequest.PartitionData(topicId, 10, 110, 210, Optional.empty()));
            FetchSessionHandler.FetchRequestData data2 = builder2.build();
            // Should have the same session ID, and next epoch and can only use topic IDs if the partition was updated.
            boolean updated = partition == 0;
            // The receiving broker will handle closing the session.
            assertEquals(123, data2.metadata().sessionId(), "Did not use same session when " + testType);
            assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch when " + testType);
            assertEquals(updated, data2.canUseTopicIds());
        });
    }

    @Test
    public void testIdUsageRevokedOnIdDowngrade() {
        // We want to test removing topic ID from an existing partition and adding a new partition without an ID in the incremental request.
        // 0 is the existing partition and 1 is the new one.
        List<Integer> partitions = Arrays.asList(0, 1);
        partitions.forEach(partition -> {
            String testType = partition == 0 ? "updating a partition" : "adding a new partition";
            Uuid fooId = Uuid.randomUuid();
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            FetchSessionHandler.Builder builder = handler.newBuilder();
            builder.add(new TopicPartition("foo", 0),
                    new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            FetchSessionHandler.FetchRequestData data = builder.build();
            assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200)),
                    data.toSend(), data.sessionPartitions());
            assertTrue(data.metadata().isFull());
            assertTrue(data.canUseTopicIds());

            FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                    respMap(new RespEntry("foo", 0, fooId, 10, 20)));
            handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

            // Try to remove a topic ID from an existing topic partition (0) or add a new topic partition (1) without an ID.
            FetchSessionHandler.Builder builder2 = handler.newBuilder();
            builder2.add(new TopicPartition("foo", partition),
                    new FetchRequest.PartitionData(Uuid.ZERO_UUID, 10, 110, 210, Optional.empty()));
            FetchSessionHandler.FetchRequestData data2 = builder2.build();
            // Should have the same session ID, and next epoch and can no longer use topic IDs.
            // The receiving broker will handle closing the session.
            assertEquals(123, data2.metadata().sessionId(), "Did not use same session when " + testType);
            assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch when " + testType);
            assertFalse(data2.canUseTopicIds());
        });
    }

    private static Stream<Arguments> idUsageCombinations() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("idUsageCombinations")
    public void testTopicIdReplaced(boolean startsWithTopicIds, boolean endsWithTopicIds) {
        TopicPartition tp = new TopicPartition("foo", 0);
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        Uuid topicId1 = startsWithTopicIds ? Uuid.randomUuid() : Uuid.ZERO_UUID;
        builder.add(tp, new FetchRequest.PartitionData(topicId1, 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", topicId1, 0, 0, 100, 200)),
                data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());
        assertEquals(startsWithTopicIds, data.canUseTopicIds());

        FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123, respMap(new RespEntry("foo", 0, topicId1, 10, 20)));
        short version = startsWithTopicIds ? ApiKeys.FETCH.latestVersion() : 12;
        handler.handleResponse(resp, version);

        // Try to add a new topic ID.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        Uuid topicId2 = endsWithTopicIds ? Uuid.randomUuid() : Uuid.ZERO_UUID;
        // Use the same data besides the topic ID.
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId2, 0, 100, 200, Optional.empty());
        builder2.add(tp, partitionData);
        FetchSessionHandler.FetchRequestData data2 = builder2.build();

        if (startsWithTopicIds && endsWithTopicIds) {
            // If we started with an ID, both a only a new ID will count towards replaced.
            // The old topic ID partition should be in toReplace, and the new one should be in toSend.
            assertEquals(Collections.singletonList(new TopicIdPartition(topicId1, tp)), data2.toReplace());
            assertMapsEqual(reqMap(new ReqEntry("foo", topicId2, 0, 0, 100, 200)),
                    data2.toSend(), data2.sessionPartitions());

            // sessionTopicNames should contain only the second topic ID.
            assertEquals(Collections.singletonMap(topicId2, tp.topic()), handler.sessionTopicNames());

        } else if (startsWithTopicIds || endsWithTopicIds) {
            // If we downgraded to not using topic IDs we will want to send this data.
            // However, we will not mark the partition as one replaced. In this scenario, we should see the session close due to
            // changing request types.
            // We will have the new topic ID in the session partition map
            assertEquals(Collections.emptyList(), data2.toReplace());
            assertMapsEqual(reqMap(new ReqEntry("foo", topicId2, 0, 0, 100, 200)),
                    data2.toSend(), data2.sessionPartitions());
            // The topicNames map will have the new topic ID if it is valid.
            // The old topic ID should be removed as the map will be empty if the request doesn't use topic IDs.
            if (endsWithTopicIds) {
                assertEquals(Collections.singletonMap(topicId2, tp.topic()), handler.sessionTopicNames());
            } else {
                assertEquals(Collections.emptyMap(), handler.sessionTopicNames());
            }

        } else {
            // Otherwise, we have no partition in toReplace and since the partition and topic ID was not updated, there is no data to send.
            assertEquals(Collections.emptyList(), data2.toReplace());
            assertEquals(Collections.emptyMap(), data2.toSend());
            assertMapsEqual(reqMap(new ReqEntry("foo", topicId2, 0, 0, 100, 200)), data2.sessionPartitions());
            // There is also nothing in the sessionTopicNames map, as there are no topic IDs used.
            assertEquals(Collections.emptyMap(), handler.sessionTopicNames());
        }

        // Should have the same session ID, and next epoch and can use topic IDs if it ended with topic IDs.
        assertEquals(123, data2.metadata().sessionId(), "Did not use same session");
        assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch");
        assertEquals(endsWithTopicIds, data2.canUseTopicIds());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSessionEpochWhenMixedUsageOfTopicIDs(boolean startsWithTopicIds) {
        Uuid fooId = startsWithTopicIds ? Uuid.randomUuid() : Uuid.ZERO_UUID;
        Uuid barId = startsWithTopicIds ? Uuid.ZERO_UUID : Uuid.randomUuid();
        short responseVersion = startsWithTopicIds ? ApiKeys.FETCH.latestVersion() : 12;

        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("bar", 1);

        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(tp0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", fooId, 0, 0, 100, 200)),
                data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());
        assertEquals(startsWithTopicIds, data.canUseTopicIds());

        FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 0, fooId, 10, 20)));
        handler.handleResponse(resp, responseVersion);

        // Re-add the first partition. Then add a partition with opposite ID usage.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(tp0, new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
        builder2.add(tp1, new FetchRequest.PartitionData(barId, 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        // Should have the same session ID, and the next epoch and can not use topic IDs.
        // The receiving broker will handle closing the session.
        assertEquals(123, data2.metadata().sessionId(), "Did not use same session");
        assertEquals(1, data2.metadata().epoch(), "Did not have final epoch");
        assertFalse(data2.canUseTopicIds());
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIdUsageWithAllForgottenPartitions(boolean useTopicIds) {
        // We want to test when all topics are removed from the session
        TopicPartition foo0 = new TopicPartition("foo", 0);
        Uuid topicId = useTopicIds ? Uuid.randomUuid() : Uuid.ZERO_UUID;
        short responseVersion = useTopicIds ? ApiKeys.FETCH.latestVersion() : 12;
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);

        // Add topic foo to the session
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(foo0, new FetchRequest.PartitionData(topicId, 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", topicId, 0, 0, 100, 200)),
                data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());
        assertEquals(useTopicIds, data.canUseTopicIds());

        FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 0, topicId, 10, 20)));
        handler.handleResponse(resp, responseVersion);

        // Remove the topic from the session
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertEquals(Collections.singletonList(new TopicIdPartition(topicId, foo0)), data2.toForget());
        // Should have the same session ID, next epoch, and same ID usage.
        assertEquals(123, data2.metadata().sessionId(), "Did not use same session when useTopicIds was " + useTopicIds);
        assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch when useTopicIds was " + useTopicIds);
        assertEquals(useTopicIds, data2.canUseTopicIds());
    }

    @Test
    public void testOkToAddNewIdAfterTopicRemovedFromSession() {
        Uuid topicId = Uuid.randomUuid();
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0),
                new FetchRequest.PartitionData(topicId, 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", topicId, 0, 0, 100, 200)),
                data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());
        assertTrue(data.canUseTopicIds());

        FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
                respMap(new RespEntry("foo", 0, topicId, 10, 20)));
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

        // Remove the partition from the session. Return a session ID as though the session is still open.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertMapsEqual(new LinkedHashMap<>(),
                data2.toSend(), data2.sessionPartitions());
        FetchResponse resp2 = FetchResponse.of(Errors.NONE, 0, 123,
                new LinkedHashMap<>());
        handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion());

        // After the topic is removed, add a recreated topic with a new ID.
        FetchSessionHandler.Builder builder3 = handler.newBuilder();
        builder3.add(new TopicPartition("foo", 0),
                new FetchRequest.PartitionData(Uuid.randomUuid(), 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data3 = builder3.build();
        // Should have the same session ID and epoch 2.
        assertEquals(123, data3.metadata().sessionId(), "Did not use same session");
        assertEquals(2, data3.metadata().epoch(), "Did not have the correct session epoch");
        assertTrue(data.canUseTopicIds());
    }

    @Test
    public void testVerifyFullFetchResponsePartitions() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        // We want to test both on older versions that do not use topic IDs and on newer versions that do.
        List<Short> versions = Arrays.asList((short) 12, ApiKeys.FETCH.latestVersion());
        versions.forEach(version -> {
            FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
            addTopicId(topicIds, topicNames, "foo", version);
            addTopicId(topicIds, topicNames, "bar", version);
            Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
            Uuid barId = topicIds.getOrDefault("bar", Uuid.ZERO_UUID);
            TopicPartition foo0 = new TopicPartition("foo", 0);
            TopicPartition foo1 = new TopicPartition("foo", 1);
            TopicPartition bar0 = new TopicPartition("bar", 0);
            FetchResponse resp1 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
                respMap(new RespEntry("foo", 0, fooId, 10, 20),
                        new RespEntry("foo", 1, fooId, 10, 20),
                        new RespEntry("bar", 0, barId, 10, 20)));
            String issue = handler.verifyFullFetchResponsePartitions(resp1.responseData(topicNames, version).keySet(),
                    resp1.topicIds(), version);
            assertTrue(issue.contains("extraPartitions="));
            assertFalse(issue.contains("omittedPartitions="));
            FetchSessionHandler.Builder builder = handler.newBuilder();
            builder.add(foo0, new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
            builder.add(foo1, new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
            builder.add(bar0, new FetchRequest.PartitionData(barId, 20, 120, 220, Optional.empty()));
            builder.build();
            FetchResponse resp2 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
                respMap(new RespEntry("foo", 0, fooId, 10, 20),
                        new RespEntry("foo", 1, fooId, 10, 20),
                        new RespEntry("bar", 0, barId, 10, 20)));
            String issue2 = handler.verifyFullFetchResponsePartitions(resp2.responseData(topicNames, version).keySet(),
                    resp2.topicIds(), version);
            assertNull(issue2);
            FetchResponse resp3 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
                respMap(new RespEntry("foo", 0, fooId, 10, 20),
                        new RespEntry("foo", 1, fooId, 10, 20)));
            String issue3 = handler.verifyFullFetchResponsePartitions(resp3.responseData(topicNames, version).keySet(),
                    resp3.topicIds(), version);
            assertFalse(issue3.contains("extraPartitions="));
            assertTrue(issue3.contains("omittedPartitions="));
        });
    }

    @Test
    public void testVerifyFullFetchResponsePartitionsWithTopicIds() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        addTopicId(topicIds, topicNames, "foo", ApiKeys.FETCH.latestVersion());
        addTopicId(topicIds, topicNames, "bar", ApiKeys.FETCH.latestVersion());
        addTopicId(topicIds, topicNames, "extra2", ApiKeys.FETCH.latestVersion());
        FetchResponse resp1 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
            respMap(new RespEntry("foo", 0, topicIds.get("foo"), 10, 20),
                    new RespEntry("extra2", 1, topicIds.get("extra2"), 10, 20),
                    new RespEntry("bar", 0, topicIds.get("bar"), 10, 20)));
        String issue = handler.verifyFullFetchResponsePartitions(resp1.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet(),
                resp1.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue.contains("extraPartitions="));
        assertFalse(issue.contains("omittedPartitions="));
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0),
                new FetchRequest.PartitionData(topicIds.get("foo"), 0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("bar", 0),
                new FetchRequest.PartitionData(topicIds.get("bar"), 20, 120, 220, Optional.empty()));
        builder.build();
        FetchResponse resp2 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
            respMap(new RespEntry("foo", 0, topicIds.get("foo"), 10, 20),
                    new RespEntry("extra2", 1, topicIds.get("extra2"), 10, 20),
                    new RespEntry("bar", 0, topicIds.get("bar"), 10, 20)));
        String issue2 = handler.verifyFullFetchResponsePartitions(resp2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet(),
                resp2.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue2.contains("extraPartitions="));
        assertFalse(issue2.contains("omittedPartitions="));
        FetchResponse resp3 = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID,
            respMap(new RespEntry("foo", 0, topicIds.get("foo"), 10, 20),
                    new RespEntry("bar", 0, topicIds.get("bar"), 10, 20)));
        String issue3 = handler.verifyFullFetchResponsePartitions(resp3.responseData(topicNames, ApiKeys.FETCH.latestVersion()).keySet(),
                resp3.topicIds(), ApiKeys.FETCH.latestVersion());
        assertNull(issue3);
    }

    @Test
    public void testTopLevelErrorResetsMetadata() {
        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        addTopicId(topicIds, topicNames, "foo", ApiKeys.FETCH.latestVersion());
        Uuid fooId = topicIds.getOrDefault("foo", Uuid.ZERO_UUID);
        builder.add(new TopicPartition("foo", 0),
                new FetchRequest.PartitionData(fooId, 0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1),
                new FetchRequest.PartitionData(fooId, 10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data.metadata().epoch());

        FetchResponse resp = FetchResponse.of(Errors.NONE, 0, 123,
            respMap(new RespEntry("foo", 0, topicIds.get("foo"), 10, 20),
                    new RespEntry("foo", 1, topicIds.get("foo"), 10, 20)));
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

        // Test an incremental fetch request which adds an ID unknown to the broker.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        addTopicId(topicIds, topicNames, "unknown", ApiKeys.FETCH.latestVersion());
        builder2.add(new TopicPartition("unknown", 0),
                new FetchRequest.PartitionData(topicIds.getOrDefault("unknown", Uuid.ZERO_UUID), 0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertFalse(data2.metadata().isFull());
        assertEquals(123, data2.metadata().sessionId());
        assertEquals(FetchMetadata.nextEpoch(INITIAL_EPOCH), data2.metadata().epoch());

        // Return and handle a response with a top level error
        FetchResponse resp2 = FetchResponse.of(Errors.UNKNOWN_TOPIC_ID, 0, 123,
            respMap(new RespEntry("unknown", 0, Uuid.randomUuid(), Errors.UNKNOWN_TOPIC_ID)));
        assertFalse(handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion()));

        // Ensure we start with a new epoch. This will close the session in the next request.
        FetchSessionHandler.Builder builder3 = handler.newBuilder();
        FetchSessionHandler.FetchRequestData data3 = builder3.build();
        assertEquals(123, data3.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data3.metadata().epoch());
    }

    private void addTopicId(Map<String, Uuid> topicIds, Map<Uuid, String> topicNames, String name, short version) {
        if (version >= 13) {
            Uuid id = Uuid.randomUuid();
            topicIds.put(name, id);
            topicNames.put(id, name);
        }
    }
}
