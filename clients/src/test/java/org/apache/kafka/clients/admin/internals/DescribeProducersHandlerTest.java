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

import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.message.DescribeProducersResponseData.ProducerState;
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DescribeProducersHandlerTest {
    private DescribeProducersHandler newHandler(
        Set<TopicPartition> topicPartitions,
        DescribeProducersOptions options
    ) {
        return new DescribeProducersHandler(
            topicPartitions,
            options,
            new LogContext()
        );
    }

    @Test
    public void testBrokerIdSetInOptions() {
        int brokerId = 3;
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 5),
            new TopicPartition("bar", 3),
            new TopicPartition("foo", 4)
        );

        DescribeProducersHandler handler = newHandler(
            topicPartitions,
            new DescribeProducersOptions().brokerId(brokerId)
        );

        AdminApiHandler.Keys<TopicPartition> keys = handler.initializeKeys();
        assertEquals(emptySet(), keys.dynamicKeys);
        assertNull(keys.lookupStrategy);

        keys.staticKeys.forEach((topicPartition, mappedBrokerId) -> {
            assertEquals(brokerId, mappedBrokerId, "Unexpected brokerId for " + topicPartition);
        });
    }

    @Test
    public void testBrokerIdNotSetInOptions() {
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 5),
            new TopicPartition("bar", 3),
            new TopicPartition("foo", 4)
        );

        DescribeProducersHandler handler = newHandler(
            topicPartitions,
            new DescribeProducersOptions()
        );

        AdminApiHandler.Keys<TopicPartition> keys = handler.initializeKeys();
        assertEquals(emptyMap(), keys.staticKeys);
        assertTrue(keys.lookupStrategy instanceof PartitionLeaderStrategy);
        assertEquals(topicPartitions, keys.dynamicKeys);
    }

    @Test
    public void testBuildRequest() {
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 5),
            new TopicPartition("bar", 3),
            new TopicPartition("foo", 4)
        );

        DescribeProducersHandler handler = newHandler(
            topicPartitions,
            new DescribeProducersOptions()
        );

        int brokerId = 3;
        DescribeProducersRequest.Builder request = handler.buildRequest(brokerId, topicPartitions);

        List<DescribeProducersRequestData.TopicRequest> topics = request.data.topics();

        assertEquals(mkSet("foo", "bar"), topics.stream()
            .map(DescribeProducersRequestData.TopicRequest::name)
            .collect(Collectors.toSet()));

        topics.forEach(topic -> {
            Set<Integer> expectedTopicPartitions = "foo".equals(topic.name()) ?
                mkSet(4, 5) : mkSet(3);
            assertEquals(expectedTopicPartitions, new HashSet<>(topic.partitionIndexes()));
        });
    }

    @Test
    public void testAuthorizationFailure() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        Throwable exception = assertFatalError(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
        assertTrue(exception instanceof TopicAuthorizationException);
        TopicAuthorizationException authException = (TopicAuthorizationException) exception;
        assertEquals(mkSet("foo"), authException.unauthorizedTopics());
    }

    @Test
    public void testInvalidTopic() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        Throwable exception = assertFatalError(topicPartition, Errors.INVALID_TOPIC_EXCEPTION);
        assertTrue(exception instanceof InvalidTopicException);
        InvalidTopicException invalidTopicException = (InvalidTopicException) exception;
        assertEquals(mkSet("foo"), invalidTopicException.invalidTopics());
    }

    @Test
    public void testUnexpectedError() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        Throwable exception = assertFatalError(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
        assertTrue(exception instanceof UnknownServerException);
    }

    @Test
    public void testRetriableErrors() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        assertRetriableError(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testUnmappedAfterNotLeaderError() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        ApiResult<TopicPartition, PartitionProducerState> result =
            handleResponseWithError(new DescribeProducersOptions(), topicPartition, Errors.NOT_LEADER_OR_FOLLOWER);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(singletonList(topicPartition), result.unmappedKeys);
    }

    @Test
    public void testFatalNotLeaderErrorIfStaticMapped() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        DescribeProducersOptions options = new DescribeProducersOptions().brokerId(1);

        ApiResult<TopicPartition, PartitionProducerState> result =
            handleResponseWithError(options, topicPartition, Errors.NOT_LEADER_OR_FOLLOWER);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(mkSet(topicPartition), result.failedKeys.keySet());
        Throwable exception = result.failedKeys.get(topicPartition);
        assertTrue(exception instanceof NotLeaderOrFollowerException);
    }

    @Test
    public void testCompletedResult() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        DescribeProducersOptions options = new DescribeProducersOptions().brokerId(1);
        DescribeProducersHandler handler = newHandler(mkSet(topicPartition), options);

        int brokerId = 3;
        PartitionResponse partitionResponse = sampleProducerState(topicPartition);
        DescribeProducersResponse response = describeProducersResponse(
            singletonMap(topicPartition, partitionResponse)
        );

        ApiResult<TopicPartition, PartitionProducerState> result =
            handler.handleResponse(brokerId, mkSet(topicPartition), response);

        assertEquals(mkSet(topicPartition), result.completedKeys.keySet());
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyList(), result.unmappedKeys);

        PartitionProducerState producerState = result.completedKeys.get(topicPartition);
        assertMatchingProducers(partitionResponse, producerState);
    }

    private void assertRetriableError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ApiResult<TopicPartition, PartitionProducerState> result =
            handleResponseWithError(new DescribeProducersOptions(), topicPartition, error);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private Throwable assertFatalError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ApiResult<TopicPartition, PartitionProducerState> result = handleResponseWithError(
            new DescribeProducersOptions(), topicPartition, error);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(mkSet(topicPartition), result.failedKeys.keySet());
        return result.failedKeys.get(topicPartition);
    }

    private ApiResult<TopicPartition, PartitionProducerState> handleResponseWithError(
        DescribeProducersOptions options,
        TopicPartition topicPartition,
        Errors error
    ) {
        DescribeProducersHandler handler = newHandler(mkSet(topicPartition), options);
        int brokerId = options.brokerId().orElse(3);
        DescribeProducersResponse response = buildResponseWithError(topicPartition, error);
        return handler.handleResponse(brokerId, mkSet(topicPartition), response);
    }

    private DescribeProducersResponse buildResponseWithError(
        TopicPartition topicPartition,
        Errors error
    ) {
        PartitionResponse partitionResponse = new PartitionResponse()
            .setPartitionIndex(topicPartition.partition())
            .setErrorCode(error.code());
        return describeProducersResponse(singletonMap(topicPartition, partitionResponse));
    }

    private PartitionResponse sampleProducerState(TopicPartition topicPartition) {
        PartitionResponse partitionResponse = new PartitionResponse()
            .setPartitionIndex(topicPartition.partition())
            .setErrorCode(Errors.NONE.code());

        partitionResponse.setActiveProducers(asList(
            new ProducerState()
                .setProducerId(12345L)
                .setProducerEpoch(15)
                .setLastSequence(75)
                .setLastTimestamp(System.currentTimeMillis())
                .setCurrentTxnStartOffset(-1L),
            new ProducerState()
                .setProducerId(98765L)
                .setProducerEpoch(30)
                .setLastSequence(150)
                .setLastTimestamp(System.currentTimeMillis() - 5000)
                .setCurrentTxnStartOffset(5000)
        ));

        return partitionResponse;
    }

    private void assertMatchingProducers(
        PartitionResponse expected,
        PartitionProducerState actual
    ) {
        List<ProducerState> expectedProducers = expected.activeProducers();
        List<org.apache.kafka.clients.admin.ProducerState> actualProducers = actual.activeProducers();

        assertEquals(expectedProducers.size(), actualProducers.size());

        Map<Long, ProducerState> expectedByProducerId = expectedProducers.stream().collect(Collectors.toMap(
            ProducerState::producerId,
            Function.identity()
        ));

        for (org.apache.kafka.clients.admin.ProducerState actualProducerState : actualProducers) {
            ProducerState expectedProducerState = expectedByProducerId.get(actualProducerState.producerId());
            assertNotNull(expectedProducerState);
            assertEquals(expectedProducerState.producerEpoch(), actualProducerState.producerEpoch());
            assertEquals(expectedProducerState.lastSequence(), actualProducerState.lastSequence());
            assertEquals(expectedProducerState.lastTimestamp(), actualProducerState.lastTimestamp());
            assertEquals(expectedProducerState.currentTxnStartOffset(),
                actualProducerState.currentTransactionStartOffset().orElse(-1L));
        }
    }

    private DescribeProducersResponse describeProducersResponse(
        Map<TopicPartition, PartitionResponse> partitionResponses
    ) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();
        Map<String, Map<Integer, PartitionResponse>> partitionResponsesByTopic =
            CollectionUtils.groupPartitionDataByTopic(partitionResponses);

        for (Map.Entry<String, Map<Integer, PartitionResponse>> topicEntry : partitionResponsesByTopic.entrySet()) {
            String topic = topicEntry.getKey();
            Map<Integer, PartitionResponse> topicPartitionResponses = topicEntry.getValue();

            TopicResponse topicResponse = new TopicResponse().setName(topic);
            response.topics().add(topicResponse);

            for (Map.Entry<Integer, PartitionResponse> partitionEntry : topicPartitionResponses.entrySet()) {
                Integer partitionId = partitionEntry.getKey();
                PartitionResponse partitionResponse = partitionEntry.getValue();
                topicResponse.partitions().add(partitionResponse.setPartitionIndex(partitionId));
            }
        }

        return new DescribeProducersResponse(response);
    }

}
