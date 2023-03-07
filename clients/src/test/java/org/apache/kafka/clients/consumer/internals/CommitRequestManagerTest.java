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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
        commitRequestManger.add(offsets);
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
        commitRequestManger.clientPoll(time.milliseconds());
        when(subscriptionState.allConsumed()).thenReturn(offsets);
        time.sleep(100);
        commitRequestManger.clientPoll(time.milliseconds());
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testAutocommitStateUponFailure() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.clientPoll(time.milliseconds());
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        time.sleep(100);
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertEquals(Long.MAX_VALUE, commitRequestManger.poll(time.milliseconds()).timeUntilNextPollMs);

        // complete the autocommit request (exceptionally)
        res.unsentRequests.get(0).future().completeExceptionally(new KafkaException("test exception"));

        // we can then autocommit again
        commitRequestManger.clientPoll(time.milliseconds());
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testEnsureStagedCommitsPurgedAfterPoll() {
        CommitRequestManager commitRequestManger = create(true, 100);
        commitRequestManger.add(new HashMap<>());
        assertEquals(1, commitRequestManger.stagedCommits().size());
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertTrue(commitRequestManger.stagedCommits().isEmpty());
    }

    @Test
    public void testAutoCommitFuture() {
        CommitRequestManager commitRequestManger = create(true, 100);
        commitRequestManger.sendAutoCommit(new HashMap<>()).complete(null);
        commitRequestManger.sendAutoCommit(new HashMap<>()).completeExceptionally(new RuntimeException("mock " +
                "exception"));
    }

    @Test
    public void testDuplicatedOffsetFetchRequestsCompletedSuccessfully() {
        CommitRequestManager commitRequestManger = create(true, 100);
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture> futures = testDuplicatedOffsetFetchRequestCompleted(
                commitRequestManger,
                partitions,
                2,
                Errors.NONE);
        futures.forEach(f -> {
            assertTrue(f.isDone());
            assertFalse(f.isCompletedExceptionally());
        });
    }

    @Test
    public void testDuplicatedOffsetFetchRequestsCompletedExceptionally() {
        CommitRequestManager commitRequestManger = create(true, 100);
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture> futures = testDuplicatedOffsetFetchRequestCompleted(
                commitRequestManger,
                partitions,
                5,
                Errors.UNKNOWN_SERVER_ERROR);
        futures.forEach(f -> {
            assertTrue(f.isCompletedExceptionally());
        });
    }

    @Test
    public void testRetriableErrorsOnDuplicatedOffsetFetchRequests() {
        CommitRequestManager commitRequestManger = create(true, 100);
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture> futures = testDuplicatedOffsetFetchRequestCompleted(
                commitRequestManger,
                partitions,
                5,
                Errors.COORDINATOR_LOAD_IN_PROGRESS);
        futures.forEach(f -> {
            assertFalse(f.isDone());
        });

        time.sleep(1000);
        commitRequestManger.poll(time.milliseconds());
        futures.forEach(f -> {
            assertFalse(f.isDone());
        });
    }

    private List<CompletableFuture> testDuplicatedOffsetFetchRequestCompleted(
            final CommitRequestManager commitRequestManger,
            final Set<TopicPartition> partitions,
            int numRequest,
            final Errors error) {
        List<CompletableFuture> futures = new ArrayList<>();

        for (int i = 0; i < numRequest; i++) {
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
            futures.add(future);
            commitRequestManger.sendFetchCommittedOffsetRequest(partitions, future);
        }

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildClientResponse(res.unsentRequests.get(0), error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }


    private CommitRequestManager create(final boolean autoCommitEnabled, final long autoCommitInterval) {
        return new CommitRequestManager(
                this.time,
                this.logContext,
                this.subscriptionState,
                new ConsumerConfig(props),
                this.coordinatorRequestManager,
                this.groupState);
    }

    private ClientResponse buildClientResponse(
            NetworkClientDelegate.UnsentRequest request,
            Errors error
    ) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof OffsetFetchRequest);
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) abstractRequest;

        OffsetFetchResponse response =
                new OffsetFetchResponse(error, new HashMap<>());
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
