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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
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
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommitRequestManagerTest {
    private SubscriptionState subscriptionState;
    private GroupState groupState;
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private Properties props;

    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = mock(SubscriptionState.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.groupState = new GroupState("group-1", Optional.empty());

        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPoll() {
        CommitRequestManager commitRequestManger = create(false, 0);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertEquals(Long.MAX_VALUE, res.timeUntilNextPollMs);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets);
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testPollAndAutoCommit() {
        CommitRequestManager commitRequestManger = create(true, 100);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertEquals(Long.MAX_VALUE, res.timeUntilNextPollMs);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        when(subscriptionState.allConsumed()).thenReturn(offsets);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testAutocommitStateUponFailure() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        time.sleep(100);
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertEquals(Long.MAX_VALUE, commitRequestManger.poll(time.milliseconds()).timeUntilNextPollMs);

        // complete the autocommit request (exceptionally)
        res.unsentRequests.get(0).future().completeExceptionally(new KafkaException("test exception"));

        // we can then autocommit again
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testEmptyUnsentOffsetCommitRequestsQueueAfterPoll() {
        CommitRequestManager commitRequestManger = create(true, 100);
        commitRequestManger.addOffsetCommitRequest(new HashMap<>());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        commitRequestManger.poll(time.milliseconds());
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testAutoCommitFuture() {
        CommitRequestManager commitRequestManger = create(true, 100);
        commitRequestManger.sendAutoCommit(new HashMap<>()).complete(null);
        commitRequestManger.sendAutoCommit(new HashMap<>()).completeExceptionally(new RuntimeException("mock " +
                "exception"));
    }

    @Test
    public void testDuplicatedSuccessfulRequests() {
        CommitRequestManager commitRequestManger = create(true, 100);
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
    public void testDuplicatedErroredRequests(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedRequests(
                commitRequestManger,
                partitions,
                5,
                error);
        // retriable will be re-queued so we only want to make sure to purge the outbound buffer for non-retriables.
        if (isRetriable)
            testRetriable(commitRequestManger, futures);
        else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManger);
        }
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
                Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false));
    }

    @ParameterizedTest
    @MethodSource("partitionDataErrorSupplier")
    public void testRetriablePartitionDataError(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
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
        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(res.unsentRequests.get(0), partitions, error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
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
                this.groupState);
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
                request.callback(),
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
