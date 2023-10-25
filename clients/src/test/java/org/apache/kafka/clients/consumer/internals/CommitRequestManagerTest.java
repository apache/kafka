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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommitRequestManagerTest {

    private SubscriptionState subscriptionState;
    private GroupState groupState;
    private LogContext logContext;
    private MembershipManager membershipManager;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private Properties props;
    private Node mockedNode = new Node(1, "host1", 9092);

    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = mock(SubscriptionState.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.membershipManager = new MembershipManagerImpl(DEFAULT_GROUP_ID, logContext);
        this.groupState = new GroupState(DEFAULT_GROUP_ID, Optional.empty());

        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPoll_SkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(false, 0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets);
        assertPoll(false, 0, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureManualCommitSent() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets);
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureAutocommitSent() {
        CommitRequestManager commitRequestManger = create(true, 100);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        when(subscriptionState.allConsumed()).thenReturn(offsets);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureCorrectInflightRequestBufferSize() {
        CommitRequestManager commitManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Create some offset commit requests
        Map<TopicPartition, OffsetAndMetadata> offsets1 = new HashMap<>();
        offsets1.put(new TopicPartition("test", 0), new OffsetAndMetadata(10L));
        offsets1.put(new TopicPartition("test", 1), new OffsetAndMetadata(20L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 = new HashMap<>();
        offsets2.put(new TopicPartition("test", 3), new OffsetAndMetadata(20L));
        offsets2.put(new TopicPartition("test", 4), new OffsetAndMetadata(20L));

        // Add the requests to the CommitRequestManager and store their futures
        ArrayList<CompletableFuture<Void>> commitFutures = new ArrayList<>();
        ArrayList<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> fetchFutures = new ArrayList<>();
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets1));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 0))));
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets2));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 1))));

        // Poll the CommitRequestManager and verify that the inflightOffsetFetches size is correct
        NetworkClientDelegate.PollResult result = commitManager.poll(time.milliseconds());
        assertEquals(4, result.unsentRequests.size());
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetCommitRequest.Builder));
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetFetchRequest.Builder));
        assertFalse(commitManager.pendingRequests.hasUnsentRequests());
        assertEquals(2, commitManager.pendingRequests.inflightOffsetFetches.size());

        // Verify that the inflight offset fetch requests have been removed from the pending request buffer
        commitFutures.forEach(f -> f.complete(null));
        fetchFutures.forEach(f -> f.complete(null));
        assertEquals(0, commitManager.pendingRequests.inflightOffsetFetches.size());
    }

    @Test
    public void testPoll_EnsureEmptyPendingRequestAfterPoll() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        commitRequestManger.addOffsetCommitRequest(new HashMap<>());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        commitRequestManger.poll(time.milliseconds());
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testAutocommit_ResendAutocommitAfterException() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<CompletableFuture<ClientResponse>> futures = assertPoll(1, commitRequestManger);
        time.sleep(99);
        // complete the autocommit request (exceptionally)
        futures.get(0).completeExceptionally(new KafkaException("test exception"));

        // we can then autocommit again
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(0, commitRequestManger);
        time.sleep(1);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(1, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testAutocommit_EnsureOnlyOneInflightRequest() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<CompletableFuture<ClientResponse>> futures = assertPoll(1, commitRequestManger);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertPoll(0, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);

        // complete the unsent request and re-poll
        futures.get(0).complete(null);
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testOffsetFetchRequest_EnsureDuplicatedRequestSucceed() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedRequests(
                commitRequestManger,
                partitions,
                2,
                Errors.NONE);
        futures.forEach(f -> {
            assertTrue(f.isDone());
            assertFalse(f.isCompletedExceptionally());
        });
        // expecting the buffers to be emptied after being completed successfully
        commitRequestManger.poll(0);
        assertEmptyPendingRequests(commitRequestManger);
    }

    @ParameterizedTest
    @MethodSource("exceptionSupplier")
    public void testOffsetFetchRequest_ErroredRequests(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedRequests(
                commitRequestManger,
                partitions,
                5,
                error);
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (isRetriable)
            testRetriable(commitRequestManger, futures);
        else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManger);
        }
    }

    @ParameterizedTest
    @MethodSource("retriableGroupErrors")
    public void testOffsetFetchFailsWithGroupErrorsAndRetriesAfterReceivingUpdatedInfoInHeartbeat(final Errors error) {
        CommitRequestManager commitRequestManger = create(true, 100);
        mockFailedOffsetFetchWaitingForNewMemberIdOrEpoch(error, commitRequestManger);

        // Mock Heartbeat received with new member ID and epoch
        String newMemberId = "new-member1";
        int newEpoch = 2;
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newMemberId, newEpoch).data());

        // A new request should be generated on the next poll, with the new member ID and epoch.
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        OffsetFetchRequestData reqData = (OffsetFetchRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals(1, reqData.groups().size());

        if (error == Errors.UNKNOWN_MEMBER_ID) {
            assertEquals(newMemberId, reqData.groups().get(0).memberId());
        } else if (error == Errors.STALE_MEMBER_EPOCH) {
            assertEquals(newEpoch, reqData.groups().get(0).memberEpoch());
        }
    }

//    @ParameterizedTest
//    @MethodSource("retriableGroupErrors")
//    public void testOffsetFetchWaitingForMemberIdAndEpochIsRetriedEvenIfMembersLeavesGroup(final Errors error) {
//        CommitRequestManager commitRequestManger = create(true, 100);
//        mockFailedOffsetFetchWaitingForNewMemberIdOrEpoch(error, commitRequestManger);
//
//        // Mock member leaves group
//        membershipManager.leaveGroup();
//
//        // A new request should be generated on the next poll, without any member ID or epoch.
//        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
//        assertEquals(1, res.unsentRequests.size());
//        assertNoMemberIdOrEpochInRequest(res.unsentRequests.get(0));
//    }

    @ParameterizedTest
    @MethodSource("retriableGroupErrors")
    public void testOffsetFetchWaitingForMemberIdAndEpochFailsIfMemberFails(final Errors error) {
        CommitRequestManager commitRequestManger = create(true, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result =
                mockFailedOffsetFetchWaitingForNewMemberIdOrEpoch(error, commitRequestManger);

        // Mock member fails
        membershipManager.transitionToFailed();

        // OffsetFetch request should fail.
        assertTrue(result.isCompletedExceptionally());

        // No other request should be generated on the next poll to the commit request manager
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
    }

    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> mockFailedOffsetFetchWaitingForNewMemberIdOrEpoch(
            Errors error, CommitRequestManager commitRequestManger) {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = commitRequestManger.addOffsetFetchRequest(partitions);

        // Complete request with retriable error for invalid member ID/epoch
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(res.unsentRequests.get(0), partitions, error));

        // Request result future should still be pending (waiting to receive a new member ID/epoch).
        assertFalse(result.isDone());

        // All outbound buffers should be empty given that it is a new request that will be retried.
        assertEquals(0, commitRequestManger.pendingRequests.inflightOffsetFetches.size());
        assertEquals(0, commitRequestManger.pendingRequests.unsentOffsetFetches.size());

        return result;
    }

    private void assertNoMemberIdOrEpochInRequest(NetworkClientDelegate.UnsentRequest req) {
        OffsetFetchRequestData reqData = (OffsetFetchRequestData) req.requestBuilder().build().data();
        assertEquals(1, reqData.groups().size());
        assertNull(reqData.groups().get(0).memberId());
        // This -1 is the default introduced by the OffsetFetchRequest.Builder when no epoch is  provided.
        assertEquals(-1, reqData.groups().get(0).memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(String memberId, int memberEpoch) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch));
    }

    private void testRetriable(final CommitRequestManager commitRequestManger,
                               final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertFalse(f.isDone()));

        time.sleep(500);
        commitRequestManger.poll(time.milliseconds());
        futures.forEach(f -> assertFalse(f.isDone()));
    }

    private void testNonRetriable(final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertTrue(f.isCompletedExceptionally()));
    }

    // Supplies (error, isRetriable)
    private static Stream<Arguments> exceptionSupplier() {
        return Stream.of(
                Arguments.of(Errors.NOT_COORDINATOR, true),
                Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, true),
                Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
                Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, false),
                Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
                Arguments.of(Errors.STALE_MEMBER_EPOCH, true),
                Arguments.of(Errors.UNKNOWN_MEMBER_ID, true));
    }

    private static Stream<Arguments> retriableGroupErrors() {
        return Stream.of(
                Arguments.of(Errors.STALE_MEMBER_EPOCH, true),
                Arguments.of(Errors.UNKNOWN_MEMBER_ID, true));
    }

    @ParameterizedTest
    @MethodSource("partitionDataErrorSupplier")
    public void testOffsetFetchRequest_PartitionDataError(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        TopicPartition tp1 = new TopicPartition("t1", 2);
        TopicPartition tp2 = new TopicPartition("t2", 3);
        partitions.add(tp1);
        partitions.add(tp2);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                commitRequestManger.addOffsetFetchRequest(partitions);

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // Setting 1 partition with error
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitionData.put(tp1, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", error));
        topicPartitionData.put(tp2, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", Errors.NONE));

        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(
                res.unsentRequests.get(0),
                topicPartitionData,
                Errors.NONE));
        if (isRetriable)
            testRetriable(commitRequestManger, Collections.singletonList(future));
        else
            testNonRetriable(Collections.singletonList(future));
    }

    private static void assertEmptyPendingRequests(CommitRequestManager commitRequestManger) {
        assertTrue(commitRequestManger.pendingRequests.inflightOffsetFetches.isEmpty());
        assertTrue(commitRequestManger.pendingRequests.unsentOffsetFetches.isEmpty());
        assertTrue(commitRequestManger.pendingRequests.unsentOffsetCommits.isEmpty());
    }

    // Supplies (error, isRetriable)
    private static Stream<Arguments> partitionDataErrorSupplier() {
        return Stream.of(
                Arguments.of(Errors.UNSTABLE_OFFSET_COMMIT, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
                Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
                Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false));
    }

    private List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> sendAndVerifyDuplicatedRequests(
            final CommitRequestManager commitRequestManger,
            final Set<TopicPartition> partitions,
            int numRequest,
            final Errors error) {
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new ArrayList<>();

        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManger.addOffsetFetchRequest(partitions));
        }

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(res.unsentRequests.get(0),
            partitions, error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }

    private List<CompletableFuture<ClientResponse>> assertPoll(
        final int numRes,
        final CommitRequestManager manager) {
        return assertPoll(true, numRes, manager);
    }

    private List<CompletableFuture<ClientResponse>> assertPoll(
        final boolean coordinatorDiscovered,
        final int numRes,
        final CommitRequestManager manager) {
        if (coordinatorDiscovered) {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        } else {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        }
        NetworkClientDelegate.PollResult res = manager.poll(time.milliseconds());
        assertEquals(numRes, res.unsentRequests.size());

        return res.unsentRequests.stream().map(NetworkClientDelegate.UnsentRequest::future).collect(Collectors.toList());
    }

    private CommitRequestManager create(final boolean autoCommitEnabled, final long autoCommitInterval) {
        props.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval));
        props.setProperty(ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommitEnabled));
        return new CommitRequestManager(
                this.time,
                this.logContext,
                this.subscriptionState,
                new ConsumerConfig(props),
                this.coordinatorRequestManager,
                this.groupState,
                this.membershipManager);
    }

    private ClientResponse buildOffsetFetchClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Set<TopicPartition> topicPartitions,
            final Errors error) {
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitions.forEach(tp -> topicPartitionData.put(tp, new OffsetFetchResponse.PartitionData(
                100L,
                Optional.of(1),
                "metadata",
                Errors.NONE)));
        return buildOffsetFetchClientResponse(request, topicPartitionData, error);
    }

    private ClientResponse buildOffsetFetchClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData,
            final Errors error) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof OffsetFetchRequest);
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) abstractRequest;
        OffsetFetchResponse response =
                new OffsetFetchResponse(error, topicPartitionData);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }
}
