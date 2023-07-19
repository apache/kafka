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

import org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy.LookupResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionLeaderStrategyTest {

    private PartitionLeaderStrategy newStrategy() {
        return new PartitionLeaderStrategy(new LogContext());
    }

    @Test
    public void testBuildLookupRequest() {
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 0),
            new TopicPartition("bar", 0),
            new TopicPartition("foo", 1),
            new TopicPartition("baz", 0)
        );

        PartitionLeaderStrategy strategy = newStrategy();

        MetadataRequest allRequest = strategy.buildRequest(topicPartitions).build();
        assertEquals(mkSet("foo", "bar", "baz"), new HashSet<>(allRequest.topics()));
        assertFalse(allRequest.allowAutoTopicCreation());

        MetadataRequest partialRequest = strategy.buildRequest(
            topicPartitions.stream().filter(tp -> tp.topic().equals("foo")).collect(Collectors.toSet())
        ).build();
        assertEquals(mkSet("foo"), new HashSet<>(partialRequest.topics()));
        assertFalse(partialRequest.allowAutoTopicCreation());
    }

    @Test
    public void testTopicAuthorizationFailure() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        Throwable exception = assertFatalTopicError(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
        assertTrue(exception instanceof TopicAuthorizationException);
        TopicAuthorizationException authException = (TopicAuthorizationException) exception;
        assertEquals(mkSet("foo"), authException.unauthorizedTopics());
    }

    @Test
    public void testInvalidTopicError() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        Throwable exception = assertFatalTopicError(topicPartition, Errors.INVALID_TOPIC_EXCEPTION);
        assertTrue(exception instanceof InvalidTopicException);
        InvalidTopicException invalidTopicException = (InvalidTopicException) exception;
        assertEquals(mkSet("foo"), invalidTopicException.invalidTopics());
    }

    @Test
    public void testUnexpectedTopicError() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        Throwable exception = assertFatalTopicError(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
        assertTrue(exception instanceof UnknownServerException);
    }

    @Test
    public void testRetriableTopicErrors() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        assertRetriableTopicError(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertRetriableTopicError(topicPartition, Errors.LEADER_NOT_AVAILABLE);
        assertRetriableTopicError(topicPartition, Errors.BROKER_NOT_AVAILABLE);
    }

    @Test
    public void testRetriablePartitionErrors() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        assertRetriablePartitionError(topicPartition, Errors.NOT_LEADER_OR_FOLLOWER);
        assertRetriablePartitionError(topicPartition, Errors.REPLICA_NOT_AVAILABLE);
        assertRetriablePartitionError(topicPartition, Errors.LEADER_NOT_AVAILABLE);
        assertRetriablePartitionError(topicPartition, Errors.BROKER_NOT_AVAILABLE);
        assertRetriablePartitionError(topicPartition, Errors.KAFKA_STORAGE_ERROR);
    }

    @Test
    public void testUnexpectedPartitionError() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        Throwable exception = assertFatalPartitionError(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
        assertTrue(exception instanceof UnknownServerException);
    }

    @Test
    public void testPartitionSuccessfullyMapped() {
        TopicPartition topicPartition1 = new TopicPartition("foo", 0);
        TopicPartition topicPartition2 = new TopicPartition("bar", 1);

        Map<TopicPartition, MetadataResponsePartition> responsePartitions = new HashMap<>(2);
        responsePartitions.put(topicPartition1, partitionResponseDataWithLeader(
            topicPartition1, 5, Arrays.asList(5, 6, 7)));
        responsePartitions.put(topicPartition2, partitionResponseDataWithLeader(
            topicPartition2, 1, Arrays.asList(2, 1, 3)));

        LookupResult<TopicPartition> result = handleLookupResponse(
            mkSet(topicPartition1, topicPartition2),
            responseWithPartitionData(responsePartitions)
        );

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(mkSet(topicPartition1, topicPartition2), result.mappedKeys.keySet());
        assertEquals(5, result.mappedKeys.get(topicPartition1));
        assertEquals(1, result.mappedKeys.get(topicPartition2));
    }

    @Test
    public void testIgnoreUnrequestedPartitions() {
        TopicPartition requestedTopicPartition = new TopicPartition("foo", 0);
        TopicPartition unrequestedTopicPartition = new TopicPartition("foo", 1);

        Map<TopicPartition, MetadataResponsePartition> responsePartitions = new HashMap<>(2);
        responsePartitions.put(requestedTopicPartition, partitionResponseDataWithLeader(
            requestedTopicPartition, 5, Arrays.asList(5, 6, 7)));
        responsePartitions.put(unrequestedTopicPartition, partitionResponseDataWithError(
            unrequestedTopicPartition, Errors.UNKNOWN_SERVER_ERROR));

        LookupResult<TopicPartition> result = handleLookupResponse(
            mkSet(requestedTopicPartition),
            responseWithPartitionData(responsePartitions)
        );

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(mkSet(requestedTopicPartition), result.mappedKeys.keySet());
        assertEquals(5, result.mappedKeys.get(requestedTopicPartition));
    }

    @Test
    public void testRetryIfLeaderUnknown() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);

        Map<TopicPartition, MetadataResponsePartition> responsePartitions = singletonMap(
            topicPartition,
            partitionResponseDataWithLeader(topicPartition, -1, Arrays.asList(5, 6, 7))
        );

        LookupResult<TopicPartition> result = handleLookupResponse(
            mkSet(topicPartition),
            responseWithPartitionData(responsePartitions)
        );

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.mappedKeys);
    }

    private void assertRetriableTopicError(
        TopicPartition topicPartition,
        Errors error
    ) {
        assertRetriableError(
            topicPartition,
            responseWithTopicError(topicPartition.topic(), error)
        );
    }

    private void assertRetriablePartitionError(
        TopicPartition topicPartition,
        Errors error
    ) {
        MetadataResponse response = responseWithPartitionData(singletonMap(
            topicPartition,
            partitionResponseDataWithError(topicPartition, error)
        ));
        assertRetriableError(topicPartition, response);
    }

    private Throwable assertFatalTopicError(
        TopicPartition topicPartition,
        Errors error
    ) {
        return assertFatalError(
            topicPartition,
            responseWithTopicError(topicPartition.topic(), error)
        );
    }

    private Throwable assertFatalPartitionError(
        TopicPartition topicPartition,
        Errors error
    ) {
        MetadataResponse response = responseWithPartitionData(singletonMap(
            topicPartition,
            partitionResponseDataWithError(topicPartition, error)
        ));
        return assertFatalError(topicPartition, response);
    }

    private void assertRetriableError(
        TopicPartition topicPartition,
        MetadataResponse response
    ) {
        LookupResult<TopicPartition> result = handleLookupResponse(mkSet(topicPartition), response);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.mappedKeys);
    }

    private Throwable assertFatalError(
        TopicPartition topicPartition,
        MetadataResponse response
    ) {
        LookupResult<TopicPartition> result = handleLookupResponse(mkSet(topicPartition), response);
        assertEquals(mkSet(topicPartition), result.failedKeys.keySet());
        return result.failedKeys.get(topicPartition);
    }

    private LookupResult<TopicPartition> handleLookupResponse(
        Set<TopicPartition> topicPartitions,
        MetadataResponse response
    ) {
        PartitionLeaderStrategy strategy = newStrategy();
        return strategy.handleResponse(topicPartitions, response);
    }

    private MetadataResponse responseWithTopicError(String topic, Errors error) {
        MetadataResponseTopic responseTopic = new MetadataResponseTopic()
            .setName(topic)
            .setErrorCode(error.code());
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.topics().add(responseTopic);
        return new MetadataResponse(responseData, ApiKeys.METADATA.latestVersion());
    }

    private MetadataResponsePartition partitionResponseDataWithError(TopicPartition topicPartition, Errors error) {
        return new MetadataResponsePartition()
            .setPartitionIndex(topicPartition.partition())
            .setErrorCode(error.code());
    }

    private MetadataResponsePartition partitionResponseDataWithLeader(
        TopicPartition topicPartition,
        Integer leaderId,
        List<Integer> replicas
    ) {
        return new MetadataResponsePartition()
            .setPartitionIndex(topicPartition.partition())
            .setErrorCode(Errors.NONE.code())
            .setLeaderId(leaderId)
            .setReplicaNodes(replicas)
            .setIsrNodes(replicas);
    }

    private MetadataResponse responseWithPartitionData(
        Map<TopicPartition, MetadataResponsePartition> responsePartitions
    ) {
        MetadataResponseData responseData = new MetadataResponseData();
        for (Map.Entry<TopicPartition, MetadataResponsePartition> entry : responsePartitions.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            MetadataResponseTopic responseTopic = responseData.topics().find(topicPartition.topic());
            if (responseTopic == null) {
                responseTopic = new MetadataResponseTopic()
                    .setName(topicPartition.topic())
                    .setErrorCode(Errors.NONE.code());
                responseData.topics().add(responseTopic);
            }
            responseTopic.partitions().add(entry.getValue());
        }
        return new MetadataResponse(responseData, ApiKeys.METADATA.latestVersion());
    }

}
