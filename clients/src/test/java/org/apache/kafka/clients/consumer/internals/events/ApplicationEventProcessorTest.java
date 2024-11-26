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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.MockRebalanceListener;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class ApplicationEventProcessorTest {
    private final Time time = new MockTime();
    private final CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
    private final ConsumerMembershipManager membershipManager = mock(ConsumerMembershipManager.class);
    private final OffsetsRequestManager offsetsRequestManager = mock(OffsetsRequestManager.class);
    private SubscriptionState subscriptionState = mock(SubscriptionState.class);
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
                Arguments.of(new PollEvent(100)),
                Arguments.of(new CreateFetchRequestsEvent(calculateDeadlineMs(12345, 100))),
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
        AutoOffsetResetStrategy strategy = AutoOffsetResetStrategy.LATEST;
        ResetOffsetEvent event = new ResetOffsetEvent(tp, strategy, 12345);

        setupProcessor(false);
        processor.process(event);
        verify(subscriptionState).requestOffsetReset(event.topicPartitions(), event.offsetResetStrategy());
    }

    @Test
    public void testSeekUnvalidatedEvent() {
        TopicPartition tp = new TopicPartition("topic", 0);
        Optional<Integer> offsetEpoch = Optional.of(1);
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                0, offsetEpoch, Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        SeekUnvalidatedEvent event = new SeekUnvalidatedEvent(12345, tp, 0, offsetEpoch);

        setupProcessor(false);
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(tp);
        doNothing().when(subscriptionState).seekUnvalidated(eq(tp), any());
        processor.process(event);
        verify(metadata).updateLastSeenEpochIfNewer(tp, offsetEpoch.get());
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
    public void testTopicSubscriptionChangeEvent() {
        Set<String> topics = Set.of("topic1", "topic2");
        Optional<ConsumerRebalanceListener> listener = Optional.of(new MockRebalanceListener());
        TopicSubscriptionChangeEvent event = new TopicSubscriptionChangeEvent(topics, listener, 12345);

        setupProcessor(true);
        when(subscriptionState.subscribe(topics, listener)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        verify(subscriptionState).subscribe(topics, listener);
        verify(metadata).requestUpdateForNewTopics();
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        // verify member state doesn't transition to JOINING.
        verify(membershipManager, never()).onConsumerPoll();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testFetchCommittedOffsetsEvent() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        TopicPartition tp2 = new TopicPartition("topic", 2);
        Set<TopicPartition> partitions = Set.of(tp0, tp1, tp2);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = Map.of(
            tp0, new OffsetAndMetadata(10L, Optional.of(2), ""),
            tp1, new OffsetAndMetadata(15L, Optional.empty(), ""),
            tp2, new OffsetAndMetadata(20L, Optional.of(3), "")
        );
        FetchCommittedOffsetsEvent event = new FetchCommittedOffsetsEvent(partitions, 12345);

        setupProcessor(true);
        when(commitRequestManager.fetchOffsets(partitions, 12345)).thenReturn(CompletableFuture.completedFuture(topicPartitionOffsets));
        processor.process(event);

        verify(commitRequestManager).fetchOffsets(partitions, 12345);
        assertEquals(topicPartitionOffsets, assertDoesNotThrow(() -> event.future().get()));
    }

    @Test
    public void testTopicSubscriptionChangeEventWithIllegalSubscriptionState() {
        subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        Optional<ConsumerRebalanceListener> listener = Optional.of(new MockRebalanceListener());
        TopicSubscriptionChangeEvent event = new TopicSubscriptionChangeEvent(
            Set.of("topic1", "topic2"), listener, 12345);

        subscriptionState.subscribe(Pattern.compile("topic.*"), listener);
        setupProcessor(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getCause().getMessage());
    }

    @Test
    public void testTopicPatternSubscriptionChangeEvent() {
        Pattern pattern = Pattern.compile("topic.*");
        Set<String> topics = Set.of("topic.1", "topic.2");
        Optional<ConsumerRebalanceListener> listener = Optional.of(new MockRebalanceListener());
        TopicPatternSubscriptionChangeEvent event = new TopicPatternSubscriptionChangeEvent(pattern, listener, 12345);

        setupProcessor(true);

        Cluster cluster = mock(Cluster.class);
        when(metadata.fetch()).thenReturn(cluster);
        when(cluster.topics()).thenReturn(topics);
        when(subscriptionState.matchesSubscribedPattern("topic.1")).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern("topic.2")).thenReturn(true);
        when(subscriptionState.subscribeFromPattern(topics)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        verify(subscriptionState).subscribe(pattern, listener);
        verify(subscriptionState).subscribeFromPattern(topics);
        verify(metadata, times(2)).requestUpdateForNewTopics();
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        // verify member state doesn't transition to JOINING.
        verify(membershipManager, never()).onConsumerPoll();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testTopicPatternSubscriptionChangeEventWithIllegalSubscriptionState() {
        subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        Optional<ConsumerRebalanceListener> listener = Optional.of(new MockRebalanceListener());
        TopicPatternSubscriptionChangeEvent event = new TopicPatternSubscriptionChangeEvent(
            Pattern.compile("topic.*"), listener, 12345);

        setupProcessor(true);

        subscriptionState.subscribe(Set.of("topic.1", "topic.2"), listener);
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getCause().getMessage());
    }

    @Test
    public void testUpdatePatternSubscriptionEventOnlyTakesEffectWhenMetadataHasNewVersion() {
        UpdatePatternSubscriptionEvent event1 = new UpdatePatternSubscriptionEvent(12345);

        setupProcessor(true);
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(metadata.updateVersion()).thenReturn(0);

        processor.process(event1);
        assertDoesNotThrow(() -> event1.future().get());

        Cluster cluster = mock(Cluster.class);
        Set<String> topics = Set.of("topic.1", "topic.2");
        when(metadata.updateVersion()).thenReturn(1);
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(metadata.fetch()).thenReturn(cluster);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(cluster.topics()).thenReturn(topics);
        when(subscriptionState.matchesSubscribedPattern("topic.1")).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern("topic.2")).thenReturn(true);
        when(subscriptionState.subscribeFromPattern(topics)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);

        UpdatePatternSubscriptionEvent event2 = new UpdatePatternSubscriptionEvent(12345);
        processor.process(event2);
        verify(metadata).requestUpdateForNewTopics();
        verify(subscriptionState).subscribeFromPattern(topics);
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        assertDoesNotThrow(() -> event2.future().get());
    }

    @Test
    public void testR2JPatternSubscriptionEventSuccess() {
        SubscriptionPattern pattern = new SubscriptionPattern("t*");
        Optional<ConsumerRebalanceListener> listener = Optional.of(mock(ConsumerRebalanceListener.class));
        TopicRe2JPatternSubscriptionChangeEvent event =
            new TopicRe2JPatternSubscriptionChangeEvent(pattern, listener, 12345);

        setupProcessor(true);
        processor.process(event);

        verify(subscriptionState).subscribe(pattern, listener);
        verify(subscriptionState, never()).subscribeFromPattern(any());
        verify(membershipManager).onSubscriptionUpdated();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testR2JPatternSubscriptionEventFailureWithMixedSubscriptionType() {
        SubscriptionPattern pattern = new SubscriptionPattern("t*");
        Optional<ConsumerRebalanceListener> listener = Optional.of(mock(ConsumerRebalanceListener.class));
        TopicRe2JPatternSubscriptionChangeEvent event =
            new TopicRe2JPatternSubscriptionChangeEvent(pattern, listener, 12345);
        Exception mixedSubscriptionError = new IllegalStateException("Subscription to topics, partitions and " +
            "pattern are mutually exclusive");
        doThrow(mixedSubscriptionError).when(subscriptionState).subscribe(pattern, listener);

        setupProcessor(true);
        processor.process(event);

        verify(subscriptionState).subscribe(pattern, listener);
        Exception thrown = assertFutureThrows(event.future(), mixedSubscriptionError.getClass());
        assertEquals(mixedSubscriptionError, thrown);
    }

    @ParameterizedTest
    @MethodSource("offsetsGenerator")
    public void testSyncCommitEvent(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        SyncCommitEvent event = new SyncCommitEvent(offsets, 12345);

        setupProcessor(true);
        doReturn(CompletableFuture.completedFuture(offsets.orElse(Map.of()))).when(commitRequestManager).commitSync(offsets, 12345);

        processor.process(event);
        verify(commitRequestManager).commitSync(offsets, 12345);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(offsets.orElse(Map.of()), committedOffsets);
    }

    @Test
    public void testSyncCommitEventWithoutCommitRequestManager() {
        SyncCommitEvent event = new SyncCommitEvent(Optional.empty(), 12345);

        setupProcessor(false);
        processor.process(event);
        assertFutureThrows(event.future(), KafkaException.class);
    }

    @Test
    public void testSyncCommitEventWithException() {
        SyncCommitEvent event = new SyncCommitEvent(Optional.empty(), 12345);

        setupProcessor(true);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException());
        doReturn(future).when(commitRequestManager).commitSync(any(), anyLong());
        processor.process(event);

        verify(commitRequestManager).commitSync(Optional.empty(), 12345);
        assertFutureThrows(event.future(), IllegalStateException.class);
    }

    @ParameterizedTest
    @MethodSource("offsetsGenerator")
    public void testAsyncCommitEventWithOffsets(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        AsyncCommitEvent event = new AsyncCommitEvent(offsets);

        setupProcessor(true);
        doReturn(CompletableFuture.completedFuture(offsets.orElse(Map.of()))).when(commitRequestManager).commitAsync(offsets);

        processor.process(event);
        verify(commitRequestManager).commitAsync(offsets);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(offsets.orElse(Map.of()), committedOffsets);
    }

    @Test
    public void testAsyncCommitEventWithoutCommitRequestManager() {
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.empty());

        setupProcessor(false);
        processor.process(event);
        assertFutureThrows(event.future(), KafkaException.class);
    }

    @Test
    public void testAsyncCommitEventWithException() {
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.empty());

        setupProcessor(true);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException());
        doReturn(future).when(commitRequestManager).commitAsync(any());
        processor.process(event);

        verify(commitRequestManager).commitAsync(Optional.empty());
        assertFutureThrows(event.future(), IllegalStateException.class);
    }

    private static Stream<Arguments> offsetsGenerator() {
        return Stream.of(
            Arguments.of(Optional.empty()),
            Arguments.of(Optional.of(Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10, Optional.of(1), ""))))
        );
    }

    private List<NetworkClientDelegate.UnsentRequest> mockCommitResults() {
        return Collections.singletonList(mock(NetworkClientDelegate.UnsentRequest.class));
    }
}
