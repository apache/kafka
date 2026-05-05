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

package org.apache.kafka.server.partition;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.OperationNotAttemptedException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterPartitionResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.util.MockScheduler;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AlterPartitionManagerTest {
    private final Uuid topicId = Uuid.randomUuid();
    private final MockTime time = new MockTime();
    private final int brokerId = 1;

    private NodeToControllerChannelManager brokerToController;

    private final TopicIdPartition tp0 = new TopicIdPartition(topicId, 0);
    private final TopicIdPartition tp1 = new TopicIdPartition(topicId, 1);
    private final TopicIdPartition tp2 = new TopicIdPartition(topicId, 2);

    @BeforeEach
    public void setup() {
        brokerToController = mock(NodeToControllerChannelManager.class);
    }

    @Test
    public void testBasic() {
        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();
        alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));
        verify(brokerToController).start();
        verify(brokerToController).sendRequest(any(), any());
    }

    @Test
    public void testBasicWithBrokerEpoch() {
        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 101L);
        alterPartitionManager.start();
        ArrayList<BrokerState> isrWithBrokerEpoch = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            isrWithBrokerEpoch.add(new BrokerState().setBrokerId(i).setBrokerEpoch(100 + i));
        }
        alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, LeaderRecoveryState.RECOVERED, isrWithBrokerEpoch, 10));

        var expectedAlterPartitionData = new AlterPartitionRequestData()
                .setBrokerId(brokerId)
                .setBrokerEpoch(101);
        var topicData = new AlterPartitionRequestData.TopicData().setTopicId(topicId);

        ArrayList<BrokerState> newIsrWithBrokerEpoch = new ArrayList<>();
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(1).setBrokerEpoch(101));
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(2).setBrokerEpoch(102));
        newIsrWithBrokerEpoch.add(new BrokerState().setBrokerId(3).setBrokerEpoch(103));

        topicData.partitions().add(new AlterPartitionRequestData.PartitionData()
                .setPartitionIndex(0)
                .setLeaderEpoch(1)
                .setPartitionEpoch(10)
                .setNewIsrWithEpochs(newIsrWithBrokerEpoch));

        expectedAlterPartitionData.topics().add(topicData);

        verify(brokerToController).start();
        ArgumentCaptor<AbstractRequest.Builder<AlterPartitionRequest>> captor = ArgumentCaptor.captor();
        verify(brokerToController).sendRequest(captor.capture(), any());
        AlterPartitionRequest.Builder builder = (AlterPartitionRequest.Builder) captor.getValue();
        assertEquals(expectedAlterPartitionData, builder.build().data());
    }

    @ParameterizedTest
    @EnumSource(LeaderRecoveryState.class)
    public void testBasicSentLeaderRecoveryState(LeaderRecoveryState leaderRecoveryState) {
        ArgumentCaptor<AbstractRequest.Builder<AlterPartitionRequest>> requestCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();
        alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1), leaderRecoveryState, 10));
        verify(brokerToController).start();
        verify(brokerToController).sendRequest(requestCapture.capture(), any());

        var request = requestCapture.getValue().build();
        assertEquals(leaderRecoveryState.value(), request.data().topics().get(0).partitions().get(0).leaderRecoveryState());
    }

    @Test
    public void testOverwriteWithinBatch() {
        ArgumentCaptor<AbstractRequest.Builder<AlterPartitionRequest>> capture = ArgumentCaptor.captor();
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();

        // Only send one ISR update for a given topic+partition
        var firstSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));
        assertFalse(firstSubmitFuture.isDone());

        var failedSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1, 2), LeaderRecoveryState.RECOVERED, 10));
        assertTrue(failedSubmitFuture.isCompletedExceptionally());
        assertFutureThrows(OperationNotAttemptedException.class, failedSubmitFuture);

        // Simulate response
        var alterPartitionResp = partitionResponse(tp0, Errors.NONE, 0, 0, 0, List.of());
        var resp = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion());

        verify(brokerToController).sendRequest(capture.capture(), callbackCapture.capture());
        callbackCapture.getValue().onComplete(resp);

        // Now we can submit this partition again
        var newSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1), LeaderRecoveryState.RECOVERED, 10));
        assertFalse(newSubmitFuture.isDone());

        verify(brokerToController).start();
        verify(brokerToController, times(2)).sendRequest(capture.capture(), callbackCapture.capture());

        // Make sure we sent the right request ISR={1}
        var request = capture.getValue().build();
        assertEquals(1, request.data().topics().size());
        if (request.version() < 3) {
            assertEquals(1, request.data().topics().get(0).partitions().get(0).newIsr().size());
        } else {
            assertEquals(1, request.data().topics().get(0).partitions().get(0).newIsrWithEpochs().size());
        }
    }

    @Test
    public void testSingleBatch() {
        ArgumentCaptor<AbstractRequest.Builder<AlterPartitionRequest>> capture = ArgumentCaptor.captor();
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();

        // First request will send batch of one
        alterPartitionManager.submit(new TopicIdPartition(topicId, 0),
                new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));

        // Other submissions will queue up until a response
        for (int i = 1; i <= 9; i++) {
            alterPartitionManager.submit(new TopicIdPartition(topicId, i),
                    new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));
        }

        // Simulate response, omitting partition 0 will allow it to stay in unsent queue
        var alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData());
        var resp = new ClientResponse(null, null, "", 0L, 0L,
                false, null, null, alterPartitionResp);

        // On the callback, we check for unsent items and send another request
        verify(brokerToController).sendRequest(capture.capture(), callbackCapture.capture());
        callbackCapture.getValue().onComplete(resp);

        verify(brokerToController).start();
        verify(brokerToController, times(2)).sendRequest(capture.capture(), callbackCapture.capture());

        // Verify the last request sent had all 10 items
        var request = capture.getValue().build();
        assertEquals(1, request.data().topics().size());
        assertEquals(10, request.data().topics().get(0).partitions().size());
    }

    @Test
    public void testSubmitFromCallback() throws ExecutionException, InterruptedException, TimeoutException {
        // prepare a partition level retriable error response
        var alterPartitionRespWithPartitionError = partitionResponse(tp0, Errors.UNKNOWN_SERVER_ERROR, 0, 0, 0, List.of());
        var errorResponse = makeClientResponse(alterPartitionRespWithPartitionError, ApiKeys.ALTER_PARTITION.latestVersion());

        var leaderId = 1;
        var leaderEpoch = 1;
        var partitionEpoch = 10;
        var isr = List.of(1, 2, 3);
        var leaderAndIsr = new LeaderAndIsr(leaderId, leaderEpoch, isr, LeaderRecoveryState.RECOVERED, partitionEpoch);
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();
        var future = alterPartitionManager.submit(tp0, leaderAndIsr);
        var finalFuture = new CompletableFuture<LeaderAndIsr>();
        future.whenComplete((result, error) -> {
            if (error != null) {
                // Retry when error.
                alterPartitionManager.submit(tp0, leaderAndIsr).whenComplete((retryResult, retryError) -> {
                    if (retryError != null) {
                        finalFuture.completeExceptionally(retryError);
                    } else {
                        finalFuture.complete(retryResult);
                    }
                });
            } else {
                finalFuture.completeExceptionally(new AssertionError("Expected to future to be failed"));
            }
        });

        verify(brokerToController).start();
        verify(brokerToController).sendRequest(any(), callbackCapture.capture());
        reset(brokerToController);
        callbackCapture.getValue().onComplete(errorResponse);

        // Complete the retry request
        var retryAlterPartitionResponse = partitionResponse(tp0, Errors.NONE, partitionEpoch, leaderId, leaderEpoch, isr);
        var retryResponse = makeClientResponse(retryAlterPartitionResponse, ApiKeys.ALTER_PARTITION.latestVersion());

        verify(brokerToController).sendRequest(any(), callbackCapture.capture());
        callbackCapture.getValue().onComplete(retryResponse);

        assertEquals(leaderAndIsr, finalFuture.get(200, TimeUnit.MILLISECONDS));
        // No more items in unsentIsrUpdates
        assertFalse(alterPartitionManager.unsentIsrUpdates.containsKey(tp0));

    }

    @Test
    public void testAuthorizationFailed() {
        testRetryOnTopLevelError(Errors.CLUSTER_AUTHORIZATION_FAILED);
    }

    @Test
    public void testStaleBrokerEpoch() {
        testRetryOnTopLevelError(Errors.STALE_BROKER_EPOCH);
    }

    @Test
    public void testUnknownServer() {
        testRetryOnTopLevelError(Errors.UNKNOWN_SERVER_ERROR);
    }

    @Test
    public void testRetryOnAuthenticationFailure() {
        testRetryOnErrorResponse(new ClientResponse(null, null, "", 0L, 0L,
                false, null, new AuthenticationException("authentication failed"), null));
    }

    @Test
    public void testRetryOnUnsupportedVersionError() {
        testRetryOnErrorResponse(new ClientResponse(null, null, "", 0L, 0L,
                false, new UnsupportedVersionException("unsupported version"), null, null));
    }

    private void testRetryOnTopLevelError(Errors error) {
        var alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData().setErrorCode(error.code()));
        var response = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion());
        testRetryOnErrorResponse(response);
    }

    private void testRetryOnErrorResponse(ClientResponse response) {
        var leaderAndIsr = new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10);
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();
        alterPartitionManager.submit(tp0, leaderAndIsr);

        verify(brokerToController).start();
        verify(brokerToController).sendRequest(any(), callbackCapture.capture());
        callbackCapture.getValue().onComplete(response);

        // Any top-level error, we want to retry, so we don't clear items from the pending map
        assertTrue(alterPartitionManager.unsentIsrUpdates.containsKey(tp0));

        reset(brokerToController);

        // After some time, we will retry failed requests
        time.sleep(100);
        scheduler.tick();

        // After a successful response, we can submit another AlterIsrItem
        var retryAlterPartitionResponse = partitionResponse(tp0, Errors.NONE, 0, 0, 0, List.of());
        var retryResponse = makeClientResponse(retryAlterPartitionResponse, ApiKeys.ALTER_PARTITION.latestVersion());

        verify(brokerToController).sendRequest(any(), callbackCapture.capture());
        callbackCapture.getValue().onComplete(retryResponse);

        assertFalse(alterPartitionManager.unsentIsrUpdates.containsKey(tp0));
    }

    @Test
    public void testInvalidUpdateVersion() {
        checkPartitionError(Errors.INVALID_UPDATE_VERSION);
    }

    @Test
    public void testUnknownTopicPartition() {
        checkPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testNotLeaderOrFollower() {
        checkPartitionError(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    public void testInvalidRequest() {
        checkPartitionError(Errors.INVALID_REQUEST);
    }

    private void checkPartitionError(Errors error) {
        var alterPartitionManager = testPartitionError(tp0, error);
        var future = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));
        assertFalse(future.isDone());
    }

    private AlterPartitionManager testPartitionError(TopicIdPartition tp, Errors error) {
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();
        reset(brokerToController);

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();

        var future = alterPartitionManager.submit(tp, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));

        verify(brokerToController).start();
        verify(brokerToController).sendRequest(any(), callbackCapture.capture());
        reset(brokerToController);

        var alterPartitionResp = partitionResponse(tp, error, 0, 0, 0, List.of());
        var resp = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion());
        callbackCapture.getValue().onComplete(resp);
        assertTrue(future.isCompletedExceptionally());
        assertFutureThrows(error.exception().getClass(), future);
        return alterPartitionManager;
    }

    @Test
    public void testOneInFlight() {
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();

        // First submit will send the request
        alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));

        // These will become pending unsent items
        alterPartitionManager.submit(tp1, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));
        alterPartitionManager.submit(tp2, new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10));

        verify(brokerToController).start();
        verify(brokerToController).sendRequest(any(), callbackCapture.capture());

        // Once the callback runs, another request will be sent
        reset(brokerToController);

        var alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData());
        var resp = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion());
        callbackCapture.getValue().onComplete(resp);

        // Verify that pending items (tp1, tp2) are sent after the callback
        verify(brokerToController).sendRequest(any(), any());
    }

    @Test
    public void testPartitionMissingInResponse() {
        var expectedVersion = ApiKeys.ALTER_PARTITION.latestVersion();
        var leaderAndIsr = new LeaderAndIsr(1, 1, List.of(1, 2, 3), LeaderRecoveryState.RECOVERED, 10);
        var scheduler = new MockScheduler(time);
        var alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, brokerId, () -> 2L);
        alterPartitionManager.start();

        // The first `submit` will send the `AlterIsr` request
        var future1 = alterPartitionManager.submit(tp0, leaderAndIsr);
        var callback1 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
                Set.of(tp0),
                expectedVersion
        ));

        // Additional calls while the `AlterIsr` request is inflight will be queued
        var future2 = alterPartitionManager.submit(tp1, leaderAndIsr);
        var future3 = alterPartitionManager.submit(tp2, leaderAndIsr);

        // Respond to the first request, which will also allow the next request to get sent
        callback1.onComplete(makeClientResponse(
                partitionResponse(tp0, Errors.UNKNOWN_SERVER_ERROR, 0, 0, 0, List.of()),
                expectedVersion
        ));
        assertFutureThrows(UnknownServerException.class, future1);
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());

        // Verify the second request includes both expected partitions, but only respond with one of them
        var callback2 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
                Set.of(tp1, tp2),
                expectedVersion
        ));
        callback2.onComplete(makeClientResponse(
                partitionResponse(tp2, Errors.UNKNOWN_SERVER_ERROR, 0, 0, 0, List.of()),
                expectedVersion
        ));
        assertFutureThrows(UnknownServerException.class, future3);
        assertFalse(future2.isDone());

        // The missing partition should be retried
        var callback3 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
                Set.of(tp1),
                expectedVersion
        ));
        callback3.onComplete(makeClientResponse(
                partitionResponse(tp1, Errors.UNKNOWN_SERVER_ERROR, 0, 0, 0, List.of()),
                expectedVersion
        ));
        assertFutureThrows(UnknownServerException.class, future2);

    }

    private ControllerRequestCompletionHandler verifySendRequest(NodeToControllerChannelManager brokerToController,
                                                                 ArgumentMatcher<AbstractRequest.Builder<? extends AbstractRequest>> expectedRequest) {
        ArgumentCaptor<ControllerRequestCompletionHandler> callbackCapture = ArgumentCaptor.captor();

        verify(brokerToController).sendRequest(argThat(expectedRequest), callbackCapture.capture());

        reset(brokerToController);

        return callbackCapture.getValue();
    }

    private ArgumentMatcher<AbstractRequest.Builder<? extends AbstractRequest>> alterPartitionRequestMatcher(Set<TopicIdPartition> expectedTopicPartitions,
                                                                                                             Short expectedVersion) {
        return request -> {
            assertEquals(ApiKeys.ALTER_PARTITION, request.apiKey());

            var alterPartitionRequest = ((AlterPartitionRequest.Builder) request).build();
            assertEquals(expectedVersion, alterPartitionRequest.version());

            var requestTopicPartitions = alterPartitionRequest.data().topics().stream()
                    .flatMap(topicData ->
                            topicData.partitions().stream()
                                    .map(partitionData ->
                                            new TopicPartitionKey(topicData.topicId(), partitionData.partitionIndex()))
                    ).collect(Collectors.toSet());

            var expectedSet = expectedTopicPartitions.stream()
                    .map(tp -> new TopicPartitionKey(tp.topicId(), tp.partitionId()))
                    .collect(Collectors.toSet());

            return expectedSet.equals(requestTopicPartitions);
        };
    }

    record TopicPartitionKey(Uuid topicId, int partitionId) { }

    private ClientResponse makeClientResponse(AlterPartitionResponse response, short version) {
        return new ClientResponse(
                new RequestHeader(response.apiKey(), version, "", 0),
                null,
                "",
                0L,
                0L,
                false,
                null,
                null,
                // Response is serialized and deserialized to ensure that its does
                // not contain ignorable fields used by other versions.
                AlterPartitionResponse.parse(MessageUtil.toByteBufferAccessor(response.data(), version), version)
        );
    }

    private AlterPartitionResponse partitionResponse(
            TopicIdPartition tp,
            Errors error,
            int partitionEpoch,
            int leaderId,
            int leaderEpoch,
            List<Integer> isr) {
        return new AlterPartitionResponse(new AlterPartitionResponseData()
                .setTopics(List.of(new AlterPartitionResponseData.TopicData()
                        .setTopicId(tp.topicId())
                        .setPartitions(List.of(new AlterPartitionResponseData.PartitionData()
                                .setPartitionIndex(tp.partitionId())
                                .setPartitionEpoch(partitionEpoch)
                                .setLeaderEpoch(leaderEpoch)
                                .setLeaderId(leaderId)
                                .setIsr(isr)
                                .setErrorCode(error.code()))))));
    }
}
