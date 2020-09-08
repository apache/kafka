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
import org.apache.kafka.clients.admin.internals.RequestDriver.RequestSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.message.DescribeProducersResponseData.ProducerState;
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.admin.internals.AdminRequestUtil.metadataResponse;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DescribeProducersRequestDriverTest {
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testSuccessfulResponseWithoutProvidedBrokerId() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        int leaderId = 1;
        DescribeProducersOptions options = new DescribeProducersOptions();

        DescribeProducersRequestDriver driver = new DescribeProducersRequestDriver(
            singleton(topicPartition),
            options,
            deadlineMs,
            retryBackoffMs
        );

        assertMetadataLookup(driver, topicPartition, leaderId, 0);

        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        RequestSpec<TopicPartition> spec = requests.get(0);
        assertEquals(OptionalInt.of(leaderId), spec.scope.destinationBrokerId());
        assertEquals(singleton(topicPartition), spec.keys);
        assertExpectedBackoffAndDeadline(spec, 0);
        assertSuccessfulFulfillment(driver, topicPartition, spec);
    }

    @Test
    public void testRetryLookupAfterNotLeaderErrorWithoutProvidedBrokerId() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        int initialLeaderId = 1;
        DescribeProducersOptions options = new DescribeProducersOptions();

        DescribeProducersRequestDriver driver = new DescribeProducersRequestDriver(
            singleton(topicPartition),
            options,
            deadlineMs,
            retryBackoffMs
        );

        assertMetadataLookup(driver, topicPartition, initialLeaderId, 0);

        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        // A `NOT_LEADER_OR_FOLLOWER` error should cause a retry of the `Metadata` request
        RequestSpec<TopicPartition> spec = requests.get(0);
        driver.onResponse(time.milliseconds(), spec, describeProducersResponse(
            singletonMap(topicPartition, new PartitionResponse()
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()))
        ));

        int updatedLeaderId = 2;
        assertMetadataLookup(driver, topicPartition, updatedLeaderId, 1);
    }

    @Test
    public void testSuccessfulResponseWithProvidedBrokerId() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        int brokerId = 1;
        DescribeProducersOptions options = new DescribeProducersOptions().setBrokerId(brokerId);

        DescribeProducersRequestDriver driver = new DescribeProducersRequestDriver(
            singleton(topicPartition),
            options,
            deadlineMs,
            retryBackoffMs
        );

        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        // Note there should be no `Metadata` lookup since we specified the target brokerId directly
        RequestSpec<TopicPartition> spec = requests.get(0);
        assertEquals(singleton(topicPartition), spec.keys);
        assertEquals(OptionalInt.of(brokerId), spec.scope.destinationBrokerId());
        assertExpectedBackoffAndDeadline(spec, 0);

        assertTrue(spec.request instanceof DescribeProducersRequest.Builder);
        DescribeProducersRequest.Builder request = (DescribeProducersRequest.Builder) spec.request;
        assertEquals(1, request.data.topics().size());
        TopicRequest topicRequest = request.data.topics().get(0);
        assertEquals(topicPartition.topic(), topicRequest.name());
        assertEquals(singletonList(topicPartition.partition()), topicRequest.partitionIndexes());
        assertSuccessfulFulfillment(driver, topicPartition, spec);
    }

    @Test
    public void testNotLeaderErrorWithProvidedBrokerId() {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        int brokerId = 1;
        DescribeProducersOptions options = new DescribeProducersOptions().setBrokerId(brokerId);

        DescribeProducersRequestDriver driver = new DescribeProducersRequestDriver(
            singleton(topicPartition),
            options,
            deadlineMs,
            retryBackoffMs
        );

        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        // Note there should be no `Metadata` lookup since we specified the target brokerId directly
        RequestSpec<TopicPartition> spec = requests.get(0);
        assertEquals(singleton(topicPartition), spec.keys);
        assertEquals(OptionalInt.of(brokerId), spec.scope.destinationBrokerId());
        assertExpectedBackoffAndDeadline(spec, 0);
        assertTrue(spec.request instanceof DescribeProducersRequest.Builder);

        driver.onResponse(time.milliseconds(), spec, describeProducersResponse(singletonMap(topicPartition,
            new PartitionResponse().setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
        )));

        assertEquals(Collections.emptyList(), driver.poll());
        assertFutureThrows(driver.futures().get(topicPartition), NotLeaderOrFollowerException.class);
    }

    @Test
    public void testFatalErrorWithoutProvidedBrokerId() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        int leaderId = 1;
        DescribeProducersOptions options = new DescribeProducersOptions();

        DescribeProducersRequestDriver driver = new DescribeProducersRequestDriver(
            singleton(topicPartition),
            options,
            deadlineMs,
            retryBackoffMs
        );

        assertMetadataLookup(driver, topicPartition, leaderId, 0);

        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        RequestSpec<TopicPartition> spec = requests.get(0);
        driver.onResponse(time.milliseconds(), spec, describeProducersResponse(
            singletonMap(topicPartition, new PartitionResponse()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()))
        ));

        assertEquals(Collections.emptyList(), driver.poll());
        assertFutureThrows(driver.futures().get(topicPartition), UnknownServerException.class);
    }

    private void assertSuccessfulFulfillment(
        DescribeProducersRequestDriver driver,
        TopicPartition topicPartition,
        RequestSpec<TopicPartition> describeProducerSpec
    ) throws Exception {
        List<ProducerState> activeProducers = sampleProducerState();
        driver.onResponse(time.milliseconds(), describeProducerSpec, describeProducersResponse(
            singletonMap(topicPartition, new PartitionResponse()
                .setErrorCode(Errors.NONE.code())
                .setActiveProducers(activeProducers)
            )));

        assertEquals(Collections.emptyList(), driver.poll());
        KafkaFutureImpl<PartitionProducerState> future = driver.futures().get(topicPartition);
        assertTrue(future.isDone());
        PartitionProducerState partitionProducerState = future.get();
        assertEquals(2, partitionProducerState.activeProducers().size());
        assertMatchingProducers(activeProducers, partitionProducerState.activeProducers());
    }

    private void assertMetadataLookup(
        DescribeProducersRequestDriver driver,
        TopicPartition topicPartition,
        int leaderId,
        int expectedTries
    ) {
        List<RequestSpec<TopicPartition>> lookupRequests = driver.poll();
        assertEquals(1, lookupRequests.size());

        RequestSpec<TopicPartition> lookupSpec = lookupRequests.get(0);
        assertEquals(OptionalInt.empty(), lookupSpec.scope.destinationBrokerId());
        assertEquals(singleton(topicPartition), lookupSpec.keys);
        assertExpectedBackoffAndDeadline(lookupSpec, expectedTries);

        assertTrue(lookupSpec.request instanceof MetadataRequest.Builder);
        MetadataRequest.Builder lookupRequest = (MetadataRequest.Builder) lookupSpec.request;
        assertEquals(singletonList(topicPartition.topic()), lookupRequest.topics());

        driver.onResponse(time.milliseconds(), lookupSpec, new MetadataResponse(metadataResponse(
            singletonMap(topicPartition, new MetadataResponseData.MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(leaderId)
                .setLeaderEpoch(15)
                .setReplicaNodes(singletonList(leaderId))
                .setIsrNodes(singletonList(leaderId)))
        )));
    }

    private void assertExpectedBackoffAndDeadline(
        RequestSpec<TopicPartition> requestSpec,
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

    private List<ProducerState> sampleProducerState() {
        return asList(
            new ProducerState()
                .setProducerId(12345L)
                .setProducerEpoch(15)
                .setLastSequence(75)
                .setLastTimestamp(time.milliseconds())
                .setCurrentTxnStartTimestamp(-1L),
            new ProducerState()
                .setProducerId(98765L)
                .setProducerEpoch(30)
                .setLastSequence(150)
                .setLastTimestamp(time.milliseconds())
                .setCurrentTxnStartTimestamp(time.milliseconds())
        );
    }

    private void assertMatchingProducers(
        List<ProducerState> expected,
        List<org.apache.kafka.clients.admin.ProducerState> actual
    ) {
        assertEquals(expected.size(), actual.size());

        Map<Long, ProducerState> expectedByProducerId = expected.stream().collect(Collectors.toMap(
            ProducerState::producerId,
            Function.identity()
        ));

        for (org.apache.kafka.clients.admin.ProducerState actualProducerState : actual) {
            ProducerState expectedProducerState = expectedByProducerId.get(actualProducerState.producerId());
            assertNotNull(expectedProducerState);
            assertEquals(expectedProducerState.producerEpoch(), actualProducerState.producerEpoch());
            assertEquals(expectedProducerState.lastSequence(), actualProducerState.lastSequence());
            assertEquals(expectedProducerState.lastTimestamp(), actualProducerState.lastTimestamp());
            assertEquals(expectedProducerState.currentTxnStartTimestamp(),
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
