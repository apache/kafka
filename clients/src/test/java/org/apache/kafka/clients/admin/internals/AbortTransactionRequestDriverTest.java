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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AbortTransactionRequestDriverTest {
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testSuccessfulAbortTransaction() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        AbortTransactionSpec abortTransactionSpec = new AbortTransactionSpec(
            topicPartition, 12345L, 15, 4321);
        AbortTransactionRequestDriver driver = new AbortTransactionRequestDriver(abortTransactionSpec,
            deadlineMs, retryBackoffMs, logContext);
        int expectedLeaderId = 5;
        assertMetadataLookup(driver, abortTransactionSpec, expectedLeaderId, 0);
        assertWriteTxnMarkers(driver, abortTransactionSpec, Errors.NONE, expectedLeaderId, 0);
        assertCompletedFuture(driver, abortTransactionSpec, Errors.NONE);
    }

    @Test
    public void testFatalTransactionCoordinatorFencedError() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        AbortTransactionSpec abortTransactionSpec = new AbortTransactionSpec(
            topicPartition, 12345L, 15, 4321);
        AbortTransactionRequestDriver driver = new AbortTransactionRequestDriver(abortTransactionSpec,
            deadlineMs, retryBackoffMs, logContext);
        int expectedLeaderId = 5;
        assertMetadataLookup(driver, abortTransactionSpec, expectedLeaderId, 0);
        assertWriteTxnMarkers(driver, abortTransactionSpec, Errors.TRANSACTION_COORDINATOR_FENCED, expectedLeaderId, 0);
        assertCompletedFuture(driver, abortTransactionSpec, Errors.TRANSACTION_COORDINATOR_FENCED);
    }

    @Test
    public void testFatalClusterAuthorizationError() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        AbortTransactionSpec abortTransactionSpec = new AbortTransactionSpec(
            topicPartition, 12345L, 15, 4321);
        AbortTransactionRequestDriver driver = new AbortTransactionRequestDriver(abortTransactionSpec,
            deadlineMs, retryBackoffMs, logContext);
        int expectedLeaderId = 5;
        assertMetadataLookup(driver, abortTransactionSpec, expectedLeaderId, 0);
        assertWriteTxnMarkers(driver, abortTransactionSpec, Errors.CLUSTER_AUTHORIZATION_FAILED, expectedLeaderId, 0);
        assertCompletedFuture(driver, abortTransactionSpec, Errors.CLUSTER_AUTHORIZATION_FAILED);
    }

    @Test
    public void testRetryLookupAfterNotLeaderError() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        AbortTransactionSpec abortTransactionSpec = new AbortTransactionSpec(
            topicPartition, 12345L, 15, 4321);
        AbortTransactionRequestDriver driver = new AbortTransactionRequestDriver(abortTransactionSpec,
            deadlineMs, retryBackoffMs, logContext);

        int initialLeaderId = 5;
        assertMetadataLookup(driver, abortTransactionSpec, initialLeaderId, 0);
        assertWriteTxnMarkers(driver, abortTransactionSpec, Errors.NOT_LEADER_OR_FOLLOWER, initialLeaderId, 0);

        int updatedLeaderId = 3;
        assertMetadataLookup(driver, abortTransactionSpec, updatedLeaderId, 1);
        assertWriteTxnMarkers(driver, abortTransactionSpec, Errors.NONE, updatedLeaderId, 0);
        assertCompletedFuture(driver, abortTransactionSpec, Errors.NONE);
    }

    private void assertMetadataLookup(
        AbortTransactionRequestDriver driver,
        AbortTransactionSpec abortTransactionSpec,
        int expectedLeaderId,
        int expectedTries
    ) {
        TopicPartition topicPartition = abortTransactionSpec.topicPartition();
        RequestDriver<TopicPartition, Void>.RequestSpec lookupSpec = assertMetadataRequest(
            driver, topicPartition, expectedTries);
        driver.onResponse(time.milliseconds(), lookupSpec, new MetadataResponse(
            AdminRequestUtil.metadataResponse(singletonMap(topicPartition,
                new MetadataResponseData.MetadataResponsePartition()
                    .setLeaderId(expectedLeaderId)
                    .setReplicaNodes(singletonList(expectedLeaderId))
                    .setLeaderEpoch(15)
                    .setIsrNodes(singletonList(expectedLeaderId))
            ))));
    }

    private void assertWriteTxnMarkers(
        AbortTransactionRequestDriver driver,
        AbortTransactionSpec abortTransactionSpec,
        Errors error,
        int expectedLeaderId,
        int expectedTries
    ) {
        RequestDriver<TopicPartition, Void>.RequestSpec requestSpec = assertWriteTxnMarkersRequest(
            driver, abortTransactionSpec, expectedLeaderId, expectedTries);
        driver.onResponse(time.milliseconds(), requestSpec,
            writeTxnMarkersResponse(abortTransactionSpec, error));
    }

    private void assertCompletedFuture(
        AbortTransactionRequestDriver driver,
        AbortTransactionSpec abortTransactionSpec,
        Errors error
    ) throws Exception {
        assertEquals(Collections.emptyList(), driver.poll());
        KafkaFutureImpl<Void> future = driver.futures().get(abortTransactionSpec.topicPartition());
        assertTrue(future.isDone());
        if (error == Errors.NONE) {
            assertNull(future.get());
        } else {
            assertFutureThrows(future, error.exception().getClass());
        }
    }

    private WriteTxnMarkersResponse writeTxnMarkersResponse(
        AbortTransactionSpec abortSpec,
        Errors error
    ) {
        Map<TopicPartition, Errors> partitionErrors = singletonMap(abortSpec.topicPartition(), error);
        return new WriteTxnMarkersResponse(singletonMap(abortSpec.producerId(), partitionErrors));
    }

    private RequestDriver<TopicPartition, Void>.RequestSpec assertWriteTxnMarkersRequest(
        AbortTransactionRequestDriver driver,
        AbortTransactionSpec abortSpec,
        int expectedLeaderId,
        int expectedTries
    ) {
        List<RequestDriver<TopicPartition, Void>.RequestSpec> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestDriver<TopicPartition, Void>.RequestSpec requestSpec = requestSpecs.get(0);
        assertExpectedBackoffAndDeadline(requestSpec, expectedTries);
        assertEquals(OptionalInt.of(expectedLeaderId), requestSpec.scope.destinationBrokerId());

        assertTrue(requestSpec.request instanceof WriteTxnMarkersRequest.Builder);
        WriteTxnMarkersRequest.Builder request = (WriteTxnMarkersRequest.Builder) requestSpec.request;

        assertEquals(1, request.data.markers().size());
        WriteTxnMarkersRequestData.WritableTxnMarker requestTxnMarker = request.data.markers().get(0);
        assertEquals(TransactionResult.ABORT.id, requestTxnMarker.transactionResult());
        assertEquals(abortSpec.producerId(), requestTxnMarker.producerId());
        assertEquals(abortSpec.producerEpoch(), requestTxnMarker.producerEpoch());
        assertEquals(abortSpec.coordinatorEpoch(), requestTxnMarker.coordinatorEpoch());

        assertEquals(1, requestTxnMarker.topics().size());
        WriteTxnMarkersRequestData.WritableTxnMarkerTopic requestTopic = requestTxnMarker.topics().get(0);
        assertEquals(abortSpec.topicPartition().topic(), requestTopic.name());
        assertEquals(singletonList(abortSpec.topicPartition().partition()), requestTopic.partitionIndexes());

        return requestSpec;
    }

    private RequestDriver<TopicPartition, Void>.RequestSpec assertMetadataRequest(
        AbortTransactionRequestDriver driver,
        TopicPartition topicPartition,
        int expectedTries
    ) {
        List<RequestDriver<TopicPartition, Void>.RequestSpec> lookupRequests = driver.poll();
        assertEquals(1, lookupRequests.size());

        RequestDriver<TopicPartition, Void>.RequestSpec lookupSpec = lookupRequests.get(0);
        assertExpectedBackoffAndDeadline(lookupSpec, expectedTries);
        assertEquals(OptionalInt.empty(), lookupSpec.scope.destinationBrokerId());

        assertTrue(lookupSpec.request instanceof MetadataRequest.Builder);
        MetadataRequest.Builder request = (MetadataRequest.Builder) lookupSpec.request;
        assertEquals(singletonList(topicPartition.topic()), request.topics());
        return lookupSpec;
    }

    private void assertExpectedBackoffAndDeadline(
        RequestDriver<TopicPartition, Void>.RequestSpec requestSpec,
        int expectedTries
    ) {
        assertEquals(expectedTries, requestSpec.tries);
        assertEquals(deadlineMs, requestSpec.deadlineMs);
        if (expectedTries == 0) {
            assertEquals(0, requestSpec.nextAllowedTryMs);
        } else {
            assertEquals(time.milliseconds() + (expectedTries * retryBackoffMs), requestSpec.nextAllowedTryMs);
        }
    }

}