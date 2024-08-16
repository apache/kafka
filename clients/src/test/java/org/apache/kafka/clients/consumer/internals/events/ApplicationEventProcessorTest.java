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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {
    private final Time time = new MockTime();
    private final CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
    private final HeartbeatRequestManager heartbeatRequestManager = mock(HeartbeatRequestManager.class);
    private final ConsumerMembershipManager membershipManager = mock(ConsumerMembershipManager.class);
    private final OffsetsRequestManager offsetsRequestManager = mock(OffsetsRequestManager.class);
    private final SubscriptionState subscriptionState = mock(SubscriptionState.class);
    private ApplicationEventProcessor processor;

    private void setupProcessor(boolean withGroupId) {
        setupProcessor(withGroupId, time);
    }

    private void setupProcessor(boolean withGroupId, Time time) {
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
                mock(ConsumerMetadata.class),
                subscriptionState,
                time
        );
    }

    @Test
    public void testPrepClosingCommitEvents() {
        setupProcessor(true);
        List<NetworkClientDelegate.UnsentRequest> results = mockCommitResults();
        doReturn(new NetworkClientDelegate.PollResult(100, results)).when(commitRequestManager).pollOnClose();
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
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        final long currentTimeMs = 12345;
        return Stream.of(
                Arguments.of(new PollEvent(100)),
                Arguments.of(new NewTopicsMetadataUpdateRequestEvent()),
                Arguments.of(new AsyncCommitEvent(new HashMap<>())),
                Arguments.of(new SyncCommitEvent(new HashMap<>(), 500)),
                Arguments.of(new ResetPositionsEvent(500)),
                Arguments.of(new ValidatePositionsEvent(500)),
                Arguments.of(new TopicMetadataEvent("topic", Long.MAX_VALUE)),
                Arguments.of(new AssignmentChangeEvent(offset, currentTimeMs)));
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

    @Test
    public void testResetPositionsProcess() {
        ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        ResetPositionsEvent event = new ResetPositionsEvent(calculateDeadlineMs(time, 100));
        applicationEventProcessor.process(event);
        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
    }

    @Test
    public void testUpdateFetchPositionsReusesPendingOffsetFetchEvent() {
        setupProcessor(true);
        when(offsetsRequestManager.validatePositionsIfNeeded()).thenReturn(CompletableFuture.completedFuture(null));
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.singleton(new TopicPartition("topic1", 0)));
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsetsResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(any(), anyLong())).thenReturn(fetchOffsetsResult);
        long updatePositionsTimeoutMs = 0;
        long fetchOffsetsTimeoutMs = 30000;
        UpdateFetchPositionsEvent updatePositionsEvent1 = new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent1);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent1.future(), updatePositionsTimeoutMs));

        // Complete OffsetsFetch request after the first update positions event expired
        updatePositionsEvent1.future().completeExceptionally(new TimeoutException("Application event expired"));
        fetchOffsetsResult.complete(Collections.emptyMap());
        clearInvocations(commitRequestManager);

        // Next UpdateFetchPositions event with the same set of partitions should reuse the pendingOffsetFetch
        when(offsetsRequestManager.resetPositionsIfNeeded()).thenReturn(CompletableFuture.completedFuture(null));
        UpdateFetchPositionsEvent updatePositionsEvent2 = new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent2);
        assertDoesNotThrow(() -> ConsumerUtils.getResult(updatePositionsEvent2.future(), updatePositionsTimeoutMs));
        assertFalse(processor.hasPendingOffsetFetchEvent(), "Pending OffsetFetch should be " +
            "removed after being used");
        verify(commitRequestManager, never()).fetchOffsets(any(), anyLong());
    }

    @Test
    public void testUpdateFetchPositionsDoesNotReusePendingOffsetFetchEventForDifferentPartitions() {
        setupProcessor(true);
        TopicPartition tp0 = new TopicPartition("topic1", 0);
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        when(offsetsRequestManager.validatePositionsIfNeeded()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsetsResult1 =
            new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(any(), anyLong())).thenReturn(fetchOffsetsResult1);
        long updatePositionsTimeoutMs = 0;
        long fetchOffsetsTimeoutMs = 30000;
        UpdateFetchPositionsEvent updatePositionsEvent1 =
            new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
                time.milliseconds() + fetchOffsetsTimeoutMs);
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.singleton(tp0));
        processor.process(updatePositionsEvent1);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent1.future(), updatePositionsTimeoutMs));

        // Complete OffsetsFetch request after the first update positions event expired
        updatePositionsEvent1.future().completeExceptionally(new TimeoutException("Application event expired"));
        fetchOffsetsResult1.complete(Collections.singletonMap(tp0, new OffsetAndMetadata(5L)));
        clearInvocations(commitRequestManager);

        // Next UpdateFetchPositions event should not reuse the pendingOffsetFetch because it's
        // for a different set of partitions. It should issue a new offsetFetch request and
        // overwrite the pendingOffsetFetch
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.singleton(tp1));
        when(commitRequestManager.fetchOffsets(any(), anyLong())).thenReturn(new CompletableFuture<>());
        UpdateFetchPositionsEvent updatePositionsEvent2 = new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent2);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent2.future(), updatePositionsTimeoutMs));
    }

    @Test
    public void testUpdateFetchPositionsDoesNotReuseExpiredOffsetFetchEvent() {
        Time time = new MockTime();
        setupProcessor(true, time);
        when(offsetsRequestManager.validatePositionsIfNeeded()).thenReturn(CompletableFuture.completedFuture(null));
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.singleton(new TopicPartition("topic1", 0)));
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsetsResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(any(), anyLong())).thenReturn(fetchOffsetsResult);
        long updatePositionsTimeoutMs = 0;
        long fetchOffsetsTimeoutMs = 30000;
        UpdateFetchPositionsEvent updatePositionsEvent1 =
            new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
                time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent1);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent1.future(), updatePositionsTimeoutMs));

        // Expire the pendingOffsetFetch request
        time.sleep(fetchOffsetsTimeoutMs);
        clearInvocations(commitRequestManager);

        // Next UpdateFetchPositions event with the same set of partitions should not reuse the
        // expired pendingOffsetFetch
        UpdateFetchPositionsEvent updatePositionsEvent2 = new UpdateFetchPositionsEvent(time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent2);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent1.future(), updatePositionsTimeoutMs));
    }

    @ParameterizedTest
    @MethodSource("offsetFetchExceptionSupplier")
    // This clears the error only when it can propagate it (a following update positions event
    // arrives). Alternatively, we could clear the pending as soon as it fails if we propagate
    // the error to the app thread via an ErrorEvent. (this would mean that successful results of
    // pendingFetch are propagated on the next updateFetchPositions, but failed results not)
    public void testOffsetFetchErrorsClearPendingEvent(Throwable fetchException) {
        setupProcessor(true);
        when(offsetsRequestManager.validatePositionsIfNeeded()).thenReturn(CompletableFuture.completedFuture(null));
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.singleton(new TopicPartition("topic1", 0)));
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsetsResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(any(), anyLong())).thenReturn(fetchOffsetsResult);
        long updatePositionsTimeoutMs = 0;
        long fetchOffsetsTimeoutMs = 30000;
        UpdateFetchPositionsEvent updatePositionsEvent = new UpdateFetchPositionsEvent(
            time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent);
        verify(commitRequestManager).fetchOffsets(any(), anyLong());
        assertTrue(processor.hasPendingOffsetFetchEvent());
        assertThrows(TimeoutException.class,
            () -> ConsumerUtils.getResult(updatePositionsEvent.future(), updatePositionsTimeoutMs));

        // Complete OffsetsFetch request with error after the first update positions event expired
        updatePositionsEvent.future().completeExceptionally(new TimeoutException("App event expired"));
        fetchOffsetsResult.completeExceptionally(fetchException);
        clearInvocations(commitRequestManager);

        // Next UpdateFetchPositions event with the same set of partitions should reuse the pendingOffsetFetch
        UpdateFetchPositionsEvent updatePositionsEvent2 = new UpdateFetchPositionsEvent(
            time.milliseconds() + updatePositionsTimeoutMs,
            time.milliseconds() + fetchOffsetsTimeoutMs);
        processor.process(updatePositionsEvent2);
        assertThrows(fetchException.getClass(), () -> ConsumerUtils.getResult(updatePositionsEvent2.future(), updatePositionsTimeoutMs));
        assertFalse(processor.hasPendingOffsetFetchEvent(), "Pending OffsetFetch should be " +
            "removed after being used");
        verify(commitRequestManager, never()).fetchOffsets(any(), anyLong());
    }

    private static Stream<Arguments> offsetFetchExceptionSupplier() {
        return Stream.of(
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR.exception()),
            Arguments.of(Errors.REQUEST_TIMED_OUT.exception()),
            Arguments.of(Errors.UNSTABLE_OFFSET_COMMIT.exception()));
    }

    private List<NetworkClientDelegate.UnsentRequest> mockCommitResults() {
        return Collections.singletonList(mock(NetworkClientDelegate.UnsentRequest.class));
    }
}
