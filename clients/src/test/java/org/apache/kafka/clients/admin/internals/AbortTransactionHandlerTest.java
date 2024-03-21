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

import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AbortTransactionHandlerTest {
    private final LogContext logContext = new LogContext();
    private final TopicPartition topicPartition = new TopicPartition("foo", 5);
    private final AbortTransactionSpec abortSpec = new AbortTransactionSpec(
        topicPartition, 12345L, (short) 15, 4321);
    private final Node node = new Node(1, "host", 1234);

    @Test
    public void testInvalidBuildRequestCall() {
        AbortTransactionHandler handler = new AbortTransactionHandler(abortSpec, logContext);
        assertThrows(IllegalArgumentException.class, () -> handler.buildRequest(1,
            emptySet()));
        assertThrows(IllegalArgumentException.class, () -> handler.buildRequest(1,
            mkSet(new TopicPartition("foo", 1))));
        assertThrows(IllegalArgumentException.class, () -> handler.buildRequest(1,
            mkSet(topicPartition, new TopicPartition("foo", 1))));
    }

    @Test
    public void testValidBuildRequestCall() {
        AbortTransactionHandler handler = new AbortTransactionHandler(abortSpec, logContext);
        WriteTxnMarkersRequest.Builder request = handler.buildBatchedRequest(1, singleton(topicPartition));
        assertEquals(1, request.data.markers().size());

        WriteTxnMarkersRequestData.WritableTxnMarker markerRequest = request.data.markers().get(0);
        assertEquals(abortSpec.producerId(), markerRequest.producerId());
        assertEquals(abortSpec.producerEpoch(), markerRequest.producerEpoch());
        assertEquals(abortSpec.coordinatorEpoch(), markerRequest.coordinatorEpoch());
        assertEquals(1, markerRequest.topics().size());

        WriteTxnMarkersRequestData.WritableTxnMarkerTopic topicRequest = markerRequest.topics().get(0);
        assertEquals(abortSpec.topicPartition().topic(), topicRequest.name());
        assertEquals(singletonList(abortSpec.topicPartition().partition()), topicRequest.partitionIndexes());
    }

    @Test
    public void testInvalidHandleResponseCall() {
        AbortTransactionHandler handler = new AbortTransactionHandler(abortSpec, logContext);
        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        assertThrows(IllegalArgumentException.class, () -> handler.handleResponse(node,
            emptySet(), new WriteTxnMarkersResponse(response)));
        assertThrows(IllegalArgumentException.class, () -> handler.handleResponse(node,
            mkSet(new TopicPartition("foo", 1)), new WriteTxnMarkersResponse(response)));
        assertThrows(IllegalArgumentException.class, () -> handler.handleResponse(node,
            mkSet(topicPartition, new TopicPartition("foo", 1)), new WriteTxnMarkersResponse(response)));
    }

    @Test
    public void testInvalidResponse() {
        AbortTransactionHandler handler = new AbortTransactionHandler(abortSpec, logContext);

        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult();
        response.markers().add(markerResponse);
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        markerResponse.setProducerId(abortSpec.producerId());
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult();
        markerResponse.topics().add(topicResponse);
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        topicResponse.setName(abortSpec.topicPartition().topic());
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult();
        topicResponse.partitions().add(partitionResponse);
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        partitionResponse.setPartitionIndex(abortSpec.topicPartition().partition());
        topicResponse.setName(abortSpec.topicPartition().topic() + "random");
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));

        topicResponse.setName(abortSpec.topicPartition().topic());
        markerResponse.setProducerId(abortSpec.producerId() + 1);
        assertFailed(KafkaException.class, topicPartition, handler.handleResponse(node, singleton(topicPartition),
            new WriteTxnMarkersResponse(response)));
    }

    @Test
    public void testSuccessfulResponse() {
        assertCompleted(abortSpec.topicPartition(), handleWithError(abortSpec, Errors.NONE));
    }

    @Test
    public void testRetriableErrors() {
        assertUnmapped(abortSpec.topicPartition(), handleWithError(abortSpec, Errors.NOT_LEADER_OR_FOLLOWER));
        assertUnmapped(abortSpec.topicPartition(), handleWithError(abortSpec, Errors.UNKNOWN_TOPIC_OR_PARTITION));
        assertUnmapped(abortSpec.topicPartition(), handleWithError(abortSpec, Errors.REPLICA_NOT_AVAILABLE));
        assertUnmapped(abortSpec.topicPartition(), handleWithError(abortSpec, Errors.BROKER_NOT_AVAILABLE));
    }

    @Test
    public void testFatalErrors() {
        assertFailed(ClusterAuthorizationException.class, abortSpec.topicPartition(),
            handleWithError(abortSpec, Errors.CLUSTER_AUTHORIZATION_FAILED));
        assertFailed(InvalidProducerEpochException.class, abortSpec.topicPartition(),
            handleWithError(abortSpec, Errors.INVALID_PRODUCER_EPOCH));
        assertFailed(TransactionCoordinatorFencedException.class, abortSpec.topicPartition(),
            handleWithError(abortSpec, Errors.TRANSACTION_COORDINATOR_FENCED));
        assertFailed(UnknownServerException.class, abortSpec.topicPartition(),
            handleWithError(abortSpec, Errors.UNKNOWN_SERVER_ERROR));
    }

    private AdminApiHandler.ApiResult<TopicPartition, Void> handleWithError(
        AbortTransactionSpec abortSpec,
        Errors error
    ) {
        AbortTransactionHandler handler = new AbortTransactionHandler(abortSpec, logContext);

        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                .setPartitionIndex(abortSpec.topicPartition().partition())
                .setErrorCode(error.code());

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(abortSpec.topicPartition().topic());
        topicResponse.partitions().add(partitionResponse);

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                .setProducerId(abortSpec.producerId());
        markerResponse.topics().add(topicResponse);

        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        response.markers().add(markerResponse);

        return handler.handleResponse(node, singleton(abortSpec.topicPartition()),
            new WriteTxnMarkersResponse(response));
    }

    private void assertUnmapped(
        TopicPartition topicPartition,
        AdminApiHandler.ApiResult<TopicPartition, Void> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(topicPartition), result.unmappedKeys);
    }

    private void assertCompleted(
        TopicPartition topicPartition,
        AdminApiHandler.ApiResult<TopicPartition, Void> result
    ) {
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(topicPartition), result.completedKeys.keySet());
        assertNull(result.completedKeys.get(topicPartition));
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        TopicPartition topicPartition,
        AdminApiHandler.ApiResult<TopicPartition, Void> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(topicPartition), result.failedKeys.keySet());
        assertInstanceOf(expectedExceptionType, result.failedKeys.get(topicPartition));
    }

}
