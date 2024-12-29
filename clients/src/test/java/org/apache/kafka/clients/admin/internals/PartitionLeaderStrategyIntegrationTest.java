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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionLeaderStrategyIntegrationTest {
    private static final long TIMEOUT_MS = 5000;
    private static final long RETRY_BACKOFF_MS = 100;

    private static final Node NODE_1 = new Node(1, "host1", 9092);
    private static final Node NODE_2 = new Node(2, "host2", 9092);

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();

    private AdminApiDriver<TopicPartition, Void> buildDriver(
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result
    ) {
        return new AdminApiDriver<>(
            new MockApiHandler(),
            result,
            time.milliseconds() + TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS,
            logContext
        );
    }

    @Test
    public void testCachingRepeatedRequest() {
        Map<TopicPartition, Integer> partitionLeaderCache = new HashMap<>();

        TopicPartition tp0 = new TopicPartition("T", 0);
        TopicPartition tp1 = new TopicPartition("T", 1);
        Set<TopicPartition> requestKeys = Set.of(tp0, tp1);

        // First, the lookup stage needs to obtain leadership data because the cache is empty
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result =
            new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        AdminApiDriver<TopicPartition, Void> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<TopicPartition>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(requestKeys, requestSpecs.get(0).keys);

        // The cache will be populated using the leader information from this metadata response
        Map<TopicPartition, Integer> leaders = Map.of(tp0, 1, tp1, 2);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        assertFalse(result.all().get(tp0).isDone());
        assertFalse(result.all().get(tp1).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(2, partitionLeaderCache.get(tp1));

        // Second, the fulfillment stage makes the actual requests
        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());

        // On the second request, the partition leader cache already contains all the leadership
        // data so the request goes straight to the fulfillment stage
        result = new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        driver = buildDriver(result);

        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        // We can tell this is the fulfillment stage by the destination broker id being set
        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());
    }

    @Test
    public void testCachingOverlappingRequests() {
        // This test uses several requests to exercise the caching in various ways:
        // 1) for T-0 and T-1            (initially the cache is empty)
        // 2) for T-1 and T-2            (leadership data for T-1 should be cached from previous request)
        // 3) for T-0, T-1 and T-2       (all leadership data should be cached already)
        // 4) for T-0, T-1, T-2 and T-3  (just T-3 needs to be looked up)
        Map<TopicPartition, Integer> partitionLeaderCache = new HashMap<>();

        TopicPartition tp0 = new TopicPartition("T", 0);
        TopicPartition tp1 = new TopicPartition("T", 1);
        TopicPartition tp2 = new TopicPartition("T", 2);
        TopicPartition tp3 = new TopicPartition("T", 3);

        //
        // Request 1 - T-0 and T-1
        //
        Set<TopicPartition> requestKeys = Set.of(tp0, tp1);

        // First, the lookup stage needs to obtain leadership data because the cache is empty
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result =
            new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        AdminApiDriver<TopicPartition, Void> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<TopicPartition>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(requestKeys, requestSpecs.get(0).keys);

        // The cache will be populated using the leader information from this metadata response
        Map<TopicPartition, Integer> leaders = Map.of(tp0, 1, tp1, 2);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        assertFalse(result.all().get(tp0).isDone());
        assertFalse(result.all().get(tp1).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(2, partitionLeaderCache.get(tp1));

        // Second, the fulfillment stage makes the actual requests
        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());

        //
        // Request 2 - T-1 and T-2
        //
        // On the second request, the partition leader cache already contains some of the leadership data.
        // Now the lookup and fulfillment stages overlap.
        requestKeys = Set.of(tp1, tp2);
        result = new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        driver = buildDriver(result);

        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(Collections.singleton(tp2), requestSpecs.get(0).keys);
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        // The cache will be populated using the leader information from this metadata response
        leaders = Map.of(tp2, 1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_2);
        assertTrue(result.all().get(tp1).isDone());  // Already fulfilled
        assertFalse(result.all().get(tp2).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(2, partitionLeaderCache.get(tp1));
        assertEquals(1, partitionLeaderCache.get(tp2));

        // Finally, the fulfillment stage makes the actual request for the uncached topic-partition
        requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        assertTrue(result.all().get(tp1).isDone());
        assertTrue(result.all().get(tp2).isDone());

        //
        // Request 3 - T-0, T-1 and T-2
        //
        // On the third request, the partition leader cache contains all the leadership data
        requestKeys = Set.of(tp0, tp1, tp2);
        result = new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        driver = buildDriver(result);

        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        // We can tell this is the fulfillment stage by the destination broker id being set
        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());
        assertTrue(result.all().get(tp2).isDone());

        //
        // Request 4 - T-0, T-1, T-2 and T-3
        //
        // On the fourth request, the partition leader cache already contains some of the leadership data.
        // Now the lookup and fulfillment stages overlap.
        requestKeys = Set.of(tp0, tp1, tp2, tp3);
        result = new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        driver = buildDriver(result);

        requestSpecs = driver.poll();
        assertEquals(3, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(Collections.singleton(tp3), requestSpecs.get(0).keys);
        assertEquals(OptionalInt.of(1), requestSpecs.get(1).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(2).scope.destinationBrokerId());

        // The cache will be populated using the leader information from this metadata response
        leaders = Map.of(tp3, 2);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseSuccess(requestSpecs.get(1).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(2), listOffsetsResponseSuccess(requestSpecs.get(2).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());  // Already fulfilled
        assertTrue(result.all().get(tp1).isDone());  // Already fulfilled
        assertTrue(result.all().get(tp2).isDone());  // Already fulfilled
        assertFalse(result.all().get(tp3).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(2, partitionLeaderCache.get(tp1));
        assertEquals(1, partitionLeaderCache.get(tp2));
        assertEquals(2, partitionLeaderCache.get(tp3));

        // Finally, the fulfillment stage makes the actual request for the uncached topic-partition
        requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.of(2), requestSpecs.get(0).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());
        assertTrue(result.all().get(tp2).isDone());
        assertTrue(result.all().get(tp3).isDone());
    }

    @Test
    public void testNotLeaderFulfillmentError() {
        Map<TopicPartition, Integer> partitionLeaderCache = new HashMap<>();

        TopicPartition tp0 = new TopicPartition("T", 0);
        TopicPartition tp1 = new TopicPartition("T", 1);
        Set<TopicPartition> requestKeys = Set.of(tp0, tp1);

        // First, the lookup stage needs to obtain leadership data because the cache is empty
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result =
            new PartitionLeaderStrategy.PartitionLeaderFuture<>(requestKeys, partitionLeaderCache);
        AdminApiDriver<TopicPartition, Void> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<TopicPartition>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(requestKeys, requestSpecs.get(0).keys);

        // The cache will be populated using the leader information from this metadata response
        Map<TopicPartition, Integer> leaders = Map.of(tp0, 1, tp1, 2);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        assertFalse(result.all().get(tp0).isDone());
        assertFalse(result.all().get(tp1).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(2, partitionLeaderCache.get(tp1));

        // Second, the fulfillment stage makes the actual requests
        requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());
        assertEquals(OptionalInt.of(2), requestSpecs.get(1).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(1), listOffsetsResponseFailure(requestSpecs.get(1).keys, Errors.NOT_LEADER_OR_FOLLOWER), NODE_2);
        assertTrue(result.all().get(tp0).isDone());
        assertFalse(result.all().get(tp1).isDone());

        // Now the lookup occurs again - change leadership to node 1
        requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.empty(), requestSpecs.get(0).scope.destinationBrokerId());

        leaders = Map.of(tp1, 1);
        driver.onResponse(time.milliseconds(), requestSpecs.get(0), metadataResponseWithPartitionLeaders(leaders), Node.noNode());
        assertTrue(result.all().get(tp0).isDone());
        assertFalse(result.all().get(tp1).isDone());

        assertEquals(1, partitionLeaderCache.get(tp0));
        assertEquals(1, partitionLeaderCache.get(tp1));

        // And the fulfillment stage makes the actual request
        requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        assertEquals(OptionalInt.of(1), requestSpecs.get(0).scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), requestSpecs.get(0), listOffsetsResponseSuccess(requestSpecs.get(0).keys), NODE_1);
        assertTrue(result.all().get(tp0).isDone());
        assertTrue(result.all().get(tp1).isDone());
    }

    @Test
    public void testFatalLookupError() {
        TopicPartition tp0 = new TopicPartition("T", 0);
        Map<TopicPartition, Integer> partitionLeaderCache = new HashMap<>();
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result =
            new PartitionLeaderStrategy.PartitionLeaderFuture<>(Collections.singleton(tp0), partitionLeaderCache);
        AdminApiDriver<TopicPartition, Void> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<TopicPartition>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        AdminApiDriver.RequestSpec<TopicPartition> spec = requestSpecs.get(0);
        assertEquals(Collections.singleton(tp0), spec.keys);

        driver.onFailure(time.milliseconds(), spec, new UnknownServerException());
        assertTrue(result.all().get(tp0).isDone());
        TestUtils.assertFutureThrows(result.all().get(tp0), UnknownServerException.class);
        assertEquals(Collections.emptyList(), driver.poll());
    }

    @Test
    public void testRetryLookupAfterDisconnect() {
        TopicPartition tp0 = new TopicPartition("T", 0);
        Map<TopicPartition, Integer> partitionLeaderCache = new HashMap<>();
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> result =
            new PartitionLeaderStrategy.PartitionLeaderFuture<>(Collections.singleton(tp0), partitionLeaderCache);
        AdminApiDriver<TopicPartition, Void> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<TopicPartition>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        AdminApiDriver.RequestSpec<TopicPartition> spec = requestSpecs.get(0);
        assertEquals(Collections.singleton(tp0), spec.keys);

        driver.onFailure(time.milliseconds(), spec, new DisconnectException());
        List<AdminApiDriver.RequestSpec<TopicPartition>> retrySpecs = driver.poll();
        assertEquals(1, retrySpecs.size());

        AdminApiDriver.RequestSpec<TopicPartition> retrySpec = retrySpecs.get(0);
        assertEquals(Collections.singleton(tp0), retrySpec.keys);
        assertEquals(time.milliseconds(), retrySpec.nextAllowedTryMs);
        assertEquals(Collections.emptyList(), driver.poll());
    }

    private MetadataResponse metadataResponseWithPartitionLeaders(Map<TopicPartition, Integer> mapping) {
        MetadataResponseData response = new MetadataResponseData();
        mapping.forEach((tp, brokerId) -> response.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName(tp.topic())
            .setPartitions(Collections.singletonList(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(tp.partition())
                .setLeaderId(brokerId)))));
        return new MetadataResponse(response, ApiKeys.METADATA.latestVersion());
    }

    private ListOffsetsResponse listOffsetsResponseSuccess(Set<TopicPartition> keys) {
        // This structure is not quite how Kafka does it, but it works for the MockApiHandler
        ListOffsetsResponseData response = new ListOffsetsResponseData();
        keys.forEach(tp -> {
            ListOffsetsResponseData.ListOffsetsPartitionResponse partResponse =
                new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                    .setPartitionIndex(tp.partition());
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(tp.topic())
                    .setPartitions(Collections.singletonList(partResponse));
            response.topics().add(topicResponse);
        });
        return new ListOffsetsResponse(response);
    }

    private ListOffsetsResponse listOffsetsResponseFailure(Set<TopicPartition> keys, Errors error) {
        // This structure is not quite how Kafka does it, but it works for the MockApiHandler
        ListOffsetsResponseData response = new ListOffsetsResponseData();
        keys.forEach(tp -> {
            ListOffsetsResponseData.ListOffsetsPartitionResponse partResponse =
                new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                    .setPartitionIndex(tp.partition())
                    .setErrorCode(error.code());
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(tp.topic())
                    .setPartitions(Collections.singletonList(partResponse));
            response.topics().add(topicResponse);
        });
        return new ListOffsetsResponse(response);
    }

    private class MockApiHandler extends AdminApiHandler.Batched<TopicPartition, Void> {
        private final PartitionLeaderStrategy partitionLeaderStrategy = new PartitionLeaderStrategy(logContext);

        @Override
        public String apiName() {
            return "mock-api";
        }

        @Override
        public AbstractRequest.Builder<?> buildBatchedRequest(
            int brokerId,
            Set<TopicPartition> keys
        ) {
            return new MetadataRequest.Builder(new MetadataRequestData());
        }

        @Override
        public ApiResult<TopicPartition, Void> handleResponse(
            Node broker,
            Set<TopicPartition> keys,
            AbstractResponse abstractResponse
        ) {
            ListOffsetsResponse response = (ListOffsetsResponse) abstractResponse;

            Map<TopicPartition, Void> completed = new HashMap<>();
            Map<TopicPartition, Throwable> failed = new HashMap<>();
            List<TopicPartition> unmapped = new ArrayList<>();

            response.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                if (partition.errorCode() != Errors.NONE.code()) {
                    Exception exception = Errors.forCode(partition.errorCode()).exception();
                    if (exception instanceof NotLeaderOrFollowerException || exception instanceof LeaderNotAvailableException) {
                        unmapped.add(tp);
                    } else if (!(exception instanceof RetriableException)) {
                        failed.put(tp, Errors.forCode(partition.errorCode()).exception());
                    }
                } else {
                    completed.put(tp, null);
                }
            }));

            return new ApiResult<>(completed, failed, unmapped);
        }

        @Override
        public PartitionLeaderStrategy lookupStrategy() {
            return partitionLeaderStrategy;
        }
    }
}
