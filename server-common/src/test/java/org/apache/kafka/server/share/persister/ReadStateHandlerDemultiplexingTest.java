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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReadStateHandlerDemultiplexingTest {
    private final MockTime time = new MockTime();
    private final MockTimer timer = new MockTimer(time);
    private final Node coordinator = new Node(1, "localhost", 9092);
    private final ShareCoordinatorMetadataCacheHelper cacheHelper = mock(ShareCoordinatorMetadataCacheHelper.class);
    private final PersisterStateManager stateManager = new PersisterStateManager(new MockClient(time), cacheHelper, time, timer);
    private final String groupId = "group";

    @Test
    void testCombinedResponseDemultiplexedPerTopicPartition() {
        Uuid topic1 = Uuid.randomUuid();
        Uuid topic2 = Uuid.randomUuid();
        var topic1Partition0 = addHandler(topic1, 0);
        var topic1Partition1 = addHandler(topic1, 1);
        var topic2Partition0 = addHandler(topic2, 0);
        var topic2Partition1 = addHandler(topic2, 1);
        var first = partitionResult(0, Errors.NONE);
        var last = partitionResult(1, Errors.NONE);

        completeBatch(clientResponse(new ReadShareGroupStateResponseData().setResults(List.of(
            topicResult(topic2, partitionResult(0, Errors.FENCED_LEADER_EPOCH), last),
            topicResult(topic1, first, partitionResult(1, Errors.INVALID_REQUEST))))));

        assertEquals(first, assertResult(topic1Partition0, topic1, 0, Errors.NONE));
        assertTrue(assertResult(topic1Partition1, topic1, 1, Errors.INVALID_REQUEST).errorMessage().endsWith("partition-1"));
        assertTrue(assertResult(topic2Partition0, topic2, 0, Errors.FENCED_LEADER_EPOCH).errorMessage().endsWith("partition-0"));
        assertEquals(last, assertResult(topic2Partition1, topic2, 1, Errors.NONE));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 16, 256})
    void testCombinedResponseVisitsEachPartitionOnce(int partitionCount) {
        Uuid topicId = Uuid.randomUuid();
        List<PersisterStateManager.ReadStateHandler> handlers = new ArrayList<>();
        List<ReadShareGroupStateResponseData.PartitionResult> partitions = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            handlers.add(addHandler(topicId, partition));
            partitions.add(spy(partitionResult(partition, Errors.NONE)));
        }
        var topic = spy(new ReadShareGroupStateResponseData.ReadStateResult()
            .setTopicId(topicId).setPartitions(partitions));
        var data = spy(new ReadShareGroupStateResponseData().setResults(List.of(topic)));

        completeBatch(clientResponse(data));

        verify(data, times(1)).results();
        verify(topic, times(1)).partitions();
        for (var partition : partitions) {
            verify(partition, times(1)).partition();
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            assertEquals(partitions.get(partition), assertResult(handlers.get(partition), topicId, partition, Errors.NONE));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMissingResultFailsOnlyAffectedHandler(boolean missingTopic) {
        Uuid topicId = Uuid.randomUuid();
        Uuid missingTopicId = missingTopic ? Uuid.randomUuid() : topicId;
        var present = addHandler(topicId, 0);
        var missing = addHandler(missingTopicId, 1);

        completeBatch(clientResponse(new ReadShareGroupStateResponseData().setResults(List.of(
            topicResult(topicId, partitionResult(0, Errors.NONE))))));

        assertResult(present, topicId, 0, Errors.NONE);
        assertResult(missing, missingTopicId, 1, Errors.UNKNOWN_SERVER_ERROR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNullOrBodylessResponseCompletesAllHandlers(boolean nullResponse) {
        Uuid topicId = Uuid.randomUuid();
        var first = addHandler(topicId, 0);
        var second = addHandler(topicId, 1);
        ClientResponse response = nullResponse ? null : new ClientResponse(
            new RequestHeader(ApiKeys.READ_SHARE_GROUP_STATE, ApiKeys.READ_SHARE_GROUP_STATE.latestVersion(), "client", 0),
            null, coordinator.idString(), time.milliseconds(), time.milliseconds(), false, null,
            new SaslAuthenticationException("authentication failed"), null);

        completeBatch(response);

        Errors error = nullResponse ? Errors.UNKNOWN_SERVER_ERROR : Errors.SASL_AUTHENTICATION_FAILED;
        assertResult(first, topicId, 0, error);
        assertResult(second, topicId, 1, error);
    }

    @Test
    void testRetriedHandlerUsesNewResponse() throws InterruptedException {
        Uuid topicId = Uuid.randomUuid();
        when(cacheHelper.containsTopic(any())).thenReturn(true);
        when(cacheHelper.getShareCoordinator(any(), any())).thenReturn(coordinator);
        var successful = addHandler(topicId, 0);
        var retrying = addHandler(topicId, 1);

        completeBatch(clientResponse(new ReadShareGroupStateResponseData().setResults(List.of(
            topicResult(topicId, partitionResult(0, Errors.NONE), partitionResult(1, Errors.NOT_COORDINATOR))))));

        assertResult(successful, topicId, 0, Errors.NONE);
        assertFalse(retrying.result().isDone());
        assertEquals(1, timer.size());
        assertTrue(timer.advanceClock(10_000));
        completeBatch(clientResponse(new ReadShareGroupStateResponseData().setResults(List.of(
            topicResult(topicId, partitionResult(1, Errors.NONE))))));
        assertResult(retrying, topicId, 1, Errors.NONE);
        assertEquals(0, timer.size());
    }

    @Test
    void testDirectCompletionWithoutSharedIndex() {
        Uuid topicId = Uuid.randomUuid();
        var handler = newHandler(topicId, 0);
        var partition = partitionResult(0, Errors.NONE);

        handler.onComplete(clientResponse(new ReadShareGroupStateResponseData().setResults(List.of(topicResult(topicId, partition)))));

        assertEquals(partition, assertResult(handler, topicId, 0, Errors.NONE));
    }

    private PersisterStateManager.ReadStateHandler newHandler(Uuid topicId, int partition) {
        return stateManager.new ReadStateHandler(groupId, topicId, partition, 0, new CompletableFuture<>(), response -> { });
    }

    private PersisterStateManager.ReadStateHandler addHandler(Uuid topicId, int partition) {
        var handler = newHandler(topicId, partition);
        handler.addRequestToNodeMap(coordinator, handler);
        return handler;
    }

    private void completeBatch(ClientResponse response) {
        Collection<RequestAndCompletionHandler> requests = stateManager.generateRequests();
        assertEquals(1, requests.size());
        requests.iterator().next().handler.onComplete(response);
    }

    private ClientResponse clientResponse(ReadShareGroupStateResponseData data) {
        return new ClientResponse(
            new RequestHeader(ApiKeys.READ_SHARE_GROUP_STATE, ApiKeys.READ_SHARE_GROUP_STATE.latestVersion(), "client", 0),
            null, coordinator.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
            new ReadShareGroupStateResponse(data));
    }

    private static ReadShareGroupStateResponseData.PartitionResult partitionResult(int partition, Errors error) {
        return new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(partition)
            .setErrorCode(error.code())
            .setErrorMessage("partition-" + partition)
            .setStateEpoch(partition + 1)
            .setStartOffset(100L + partition)
            .setStateBatches(List.of(new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(100L + partition)
                .setLastOffset(110L + partition)
                .setDeliveryState((byte) 0)
                .setDeliveryCount((short) 2)));
    }

    private static ReadShareGroupStateResponseData.ReadStateResult topicResult(
        Uuid topicId,
        ReadShareGroupStateResponseData.PartitionResult... partitions
    ) {
        return new ReadShareGroupStateResponseData.ReadStateResult().setTopicId(topicId).setPartitions(List.of(partitions));
    }

    private static ReadShareGroupStateResponseData.PartitionResult assertResult(
        PersisterStateManager.ReadStateHandler handler,
        Uuid topicId,
        int partition,
        Errors error
    ) {
        assertTrue(handler.result().isDone());
        var response = handler.result().join();
        assertEquals(1, response.data().results().size());
        var topicResult = response.data().results().get(0);
        assertEquals(topicId, topicResult.topicId());
        assertEquals(1, topicResult.partitions().size());
        var result = topicResult.partitions().get(0);
        assertEquals(partition, result.partition());
        assertEquals(error.code(), result.errorCode());
        return result;
    }
}
