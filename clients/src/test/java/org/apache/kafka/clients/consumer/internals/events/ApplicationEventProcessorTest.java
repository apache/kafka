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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {
    private final Time time = new MockTime();
    private final CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
    private final ConsumerMembershipManager membershipManager = mock(ConsumerMembershipManager.class);
    private final OffsetsRequestManager offsetsRequestManager = mock(OffsetsRequestManager.class);
    private final SubscriptionState subscriptionState = mock(SubscriptionState.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private ApplicationEventProcessor processor;

    private void setupProcessor(boolean withGroupId) {
        RequestManagers requestManagers = new RequestManagers(
                new LogContext(),
                offsetsRequestManager,
                mock(TopicMetadataRequestManager.class),
                mock(FetchRequestManager.class),
                withGroupId ? Optional.of(mock(CoordinatorRequestManager.class)) : Optional.empty(),
                withGroupId ? Optional.of(commitRequestManager) : Optional.empty(),
                withGroupId ? Optional.of(heartbeatRequestManager) : Optional.empty(),
                withGroupId ? Optional.of(membershipManager) : Optional.empty());
        processor = new ApplicationEventProcessor(
                new LogContext(),
                requestManagers,
                metadata,
                subscriptionState
        );
    }

    @Test
    public void testPrepClosingCommitEvents() {
        setupProcessor(true);
        List<NetworkClientDelegate.UnsentRequest> results = mockCommitResults();
        doReturn(new NetworkClientDelegate.PollResult(100, results)).when(commitRequestManager).pollOnClose(anyLong());
        processor.process(new CommitOnCloseEvent());
        verify(commitRequestManager).signalClose();
    }

    @Test
    public void testProcessUnsubscribeEventWithGroupId() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(membershipManager.leaveGroup()).thenReturn(CompletableFuture.completedFuture(null));
        processor.process(new UnsubscribeEvent(0));
        verify(membershipManager).leaveGroup();
    }

    @Test
    public void testProcessUnsubscribeEventWithoutGroupId() {
        setupProcessor(false);
        processor.process(new UnsubscribeEvent(0));
        verify(subscriptionState).unsubscribe();
    }

    @ParameterizedTest
    @MethodSource("applicationEvents")
    public void testApplicationEventIsProcessed(ApplicationEvent e) {
        ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        applicationEventProcessor.process(e);
        verify(applicationEventProcessor).process(any(e.getClass()));
    }

    private static Stream<Arguments> applicationEvents() {
        return Stream.of(
                Arguments.of(new AsyncCommitEvent(new HashMap<>())),
                Arguments.of(new SyncCommitEvent(new HashMap<>(), 500)),
                Arguments.of(new CheckAndUpdatePositionsEvent(500)),
                Arguments.of(new TopicMetadataEvent("topic", Long.MAX_VALUE)),
                Arguments.of(new AssignmentChangeEvent(12345, 12345, Collections.emptyList())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, calculateDeadlineMs(time, 100), requireTimestamp);
        applicationEventProcessor.process(e);
        verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAssignmentChangeEvent(boolean withGroupId) {
        final long currentTimeMs = 12345;
        TopicPartition tp = new TopicPartition("topic", 0);
        AssignmentChangeEvent event = new AssignmentChangeEvent(currentTimeMs, 12345, Collections.singleton(tp));

        setupProcessor(withGroupId);
        doReturn(true).when(subscriptionState).assignFromUser(Collections.singleton(tp));
        processor.process(event);
        if (withGroupId) {
            verify(commitRequestManager).updateAutoCommitTimer(currentTimeMs);
            verify(commitRequestManager).maybeAutoCommitAsync();
        } else {
            verify(commitRequestManager, never()).updateAutoCommitTimer(currentTimeMs);
            verify(commitRequestManager, never()).maybeAutoCommitAsync();
        }
        verify(metadata).requestUpdateForNewTopics();
        verify(subscriptionState).assignFromUser(Collections.singleton(tp));
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testAssignmentChangeEventWithException() {
        AssignmentChangeEvent event = new AssignmentChangeEvent(12345, 12345, Collections.emptyList());

        setupProcessor(false);
        doThrow(new IllegalStateException()).when(subscriptionState).assignFromUser(any());
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
    }

    @Test
    public void testResetOffsetEvent() {
        Collection<TopicPartition> tp = Collections.singleton(new TopicPartition("topic", 0));
        OffsetResetStrategy strategy = OffsetResetStrategy.LATEST;
        ResetOffsetEvent event = new ResetOffsetEvent(tp, strategy, 12345);

        setupProcessor(false);
        processor.process(event);
        verify(subscriptionState).requestOffsetReset(event.topicPartitions(), event.offsetResetStrategy());
    }

    @Test
    public void testSeekUnvalidatedEvent() {
        TopicPartition tp = new TopicPartition("topic", 0);
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                0, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        SeekUnvalidatedEvent event = new SeekUnvalidatedEvent(12345, tp, 0, Optional.empty());

        setupProcessor(false);
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(tp);
        doNothing().when(subscriptionState).seekUnvalidated(eq(tp), any());
        processor.process(event);
        verify(metadata).currentLeader(tp);
        verify(subscriptionState).seekUnvalidated(tp, position);
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testSeekUnvalidatedEventWithException() {
        TopicPartition tp = new TopicPartition("topic", 0);
        SeekUnvalidatedEvent event = new SeekUnvalidatedEvent(12345, tp, 0, Optional.empty());

        setupProcessor(false);
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(tp);
        doThrow(new IllegalStateException()).when(subscriptionState).seekUnvalidated(eq(tp), any());
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
    }

    @Test
    public void testPollEvent() {
        PollEvent event = new PollEvent(12345);

        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);
        verify(commitRequestManager).updateAutoCommitTimer(12345);
        verify(membershipManager).onConsumerPoll();
        verify(heartbeatRequestManager).resetPollTimer(12345);
    }

    @Test
    public void testSubscriptionChangeEvent() {
        SubscriptionChangeEvent event = new SubscriptionChangeEvent();

        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);
        verify(membershipManager).onSubscriptionUpdated();
        // verify member state doesn't transition to JOINING.
        verify(membershipManager, never()).onConsumerPoll();
    }

    private List<NetworkClientDelegate.UnsentRequest> mockCommitResults() {
        return Collections.singletonList(mock(NetworkClientDelegate.UnsentRequest.class));
    }
}
