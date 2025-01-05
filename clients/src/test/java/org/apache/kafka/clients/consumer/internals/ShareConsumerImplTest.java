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

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackRegistrationEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.MockConsumerInterceptor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class ShareConsumerImplTest {

    private ShareConsumerImpl<String, String> consumer = null;

    private final Time time = new MockTime(1);
    private final ShareFetchCollector<String, String> fetchCollector = mock(ShareFetchCollector.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private final CompletableEventReaper backgroundEventReaper = mock(CompletableEventReaper.class);

    @AfterEach
    public void resetAll() {
        backgroundEventQueue.clear();
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
        consumer = null;
        Mockito.framework().clearInlineMocks();
        MockConsumerInterceptor.resetCounters();
    }

    private ShareConsumerImpl<String, String> newConsumer() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        return newConsumer(props);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ShareConsumerImpl<String, String> newConsumerWithEmptyGroupId() {
        final Properties props = requiredConsumerPropertiesAndGroupId("");
        return newConsumer(props);
    }

    private ShareConsumerImpl<String, String> newConsumer(Properties props) {
        final ConsumerConfig config = new ConsumerConfig(props);
        return newConsumer(config);
    }

    private ShareConsumerImpl<String, String> newConsumer(ConsumerConfig config) {
        return new ShareConsumerImpl<>(
                config,
                new StringDeserializer(),
                new StringDeserializer(),
                time,
                (a, b, c, d, e, f, g, h) -> applicationEventHandler,
                a -> backgroundEventReaper,
                (a, b, c, d, e) -> fetchCollector,
                backgroundEventQueue
        );
    }

    private ShareConsumerImpl<String, String> newConsumer(
        SubscriptionState subscriptions
    ) {
        return newConsumer(
                mock(ShareFetchBuffer.class),
                subscriptions,
                "group-id",
                "client-id");
    }

    private ShareConsumerImpl<String, String> newConsumer(
            ShareFetchBuffer fetchBuffer,
            SubscriptionState subscriptions,
            String groupId,
            String clientId
    ) {
        final int defaultApiTimeoutMs = 1000;

        return new ShareConsumerImpl<>(
                new LogContext(),
                clientId,
                new StringDeserializer(),
                new StringDeserializer(),
                fetchBuffer,
                fetchCollector,
                time,
                applicationEventHandler,
                backgroundEventQueue,
                backgroundEventReaper,
                new Metrics(),
                subscriptions,
                metadata,
                defaultApiTimeoutMs,
                groupId
        );
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        completeShareAcknowledgeOnCloseApplicationEventSuccessfully();
        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testInvalidGroupId() {
        KafkaException e = assertThrows(KafkaException.class, this::newConsumerWithEmptyGroupId);
        assertInstanceOf(InvalidGroupIdException.class, e.getCause());
    }

    @Test
    public void testFailConstructor() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "an.invalid.class");
        final ConsumerConfig config = new ConsumerConfig(props);
        KafkaException ce = assertThrows(
                KafkaException.class,
                () -> newConsumer(config));
        assertTrue(ce.getMessage().contains("Failed to construct Kafka share consumer"), "Unexpected exception message: " + ce.getMessage());
        assertTrue(ce.getCause().getMessage().contains("Class an.invalid.class cannot be found"), "Unexpected cause: " + ce.getCause());
    }

    @Test
    public void testWakeupBeforeCallingPoll() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        final String topicName = "foo";
        doReturn(ShareFetch.empty()).when(fetchCollector).collect(any(ShareFetchBuffer.class));

        final List<String> subscriptionTopic = Collections.singletonList(topicName);
        completeShareSubscriptionChangeApplicationEventSuccessfully(subscriptions, subscriptionTopic);
        consumer.subscribe(subscriptionTopic);

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterEmptyFetch() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        final String topicName = "foo";
        doAnswer(invocation -> {
            consumer.wakeup();
            return ShareFetch.empty();
        }).doAnswer(invocation -> ShareFetch.empty()).when(fetchCollector).collect(any(ShareFetchBuffer.class));

        final List<String> subscriptionTopic = Collections.singletonList(topicName);
        completeShareSubscriptionChangeApplicationEventSuccessfully(subscriptions, subscriptionTopic);
        consumer.subscribe(subscriptionTopic);

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofMinutes(1)));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterNonEmptyFetch() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        final String topicName = "foo";
        final int partition = 3;
        final TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), partition, topicName);
        final ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(tip);
        batch.addRecord(new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"));
        doAnswer(invocation -> {
            consumer.wakeup();
            final ShareFetch<String, String> fetch = ShareFetch.empty();
            fetch.add(tip, batch);
            return fetch;
        }).when(fetchCollector).collect(Mockito.any(ShareFetchBuffer.class));

        final List<String> subscriptionTopic = Collections.singletonList(topicName);
        completeShareSubscriptionChangeApplicationEventSuccessfully(subscriptions, subscriptionTopic);
        consumer.subscribe(subscriptionTopic);

        // since wakeup() is called when the non-empty fetch is returned the wakeup should be ignored
        assertDoesNotThrow(() -> consumer.poll(Duration.ofMinutes(1)));
        // the previously ignored wake-up should not be ignored in the next call
        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testFailOnClosedConsumer() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        completeShareAcknowledgeOnCloseApplicationEventSuccessfully();
        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);
        consumer.close();
        final IllegalStateException res = assertThrows(IllegalStateException.class, consumer::subscription);
        assertEquals("This consumer has already been closed.", res.getMessage());
    }

    @Test
    public void testVerifyApplicationEventOnShutdown() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        completeShareAcknowledgeOnCloseApplicationEventSuccessfully();
        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);
        consumer.close();
        verify(applicationEventHandler).addAndGet(any(ShareAcknowledgeOnCloseEvent.class));
        verify(applicationEventHandler).add(any(ShareUnsubscribeEvent.class));
    }

    @Test
    public void testAcknowledgementCommitCallbackRegistrationEvent() {
        consumer = newConsumer();
        AcknowledgementCommitCallback callback = mock(AcknowledgementCommitCallback.class);

        consumer.setAcknowledgementCommitCallback(callback);
        verify(applicationEventHandler).add(argThat(event ->
            event instanceof ShareAcknowledgementCommitCallbackRegistrationEvent &&
            ((ShareAcknowledgementCommitCallbackRegistrationEvent) event).isCallbackRegistered()
        ));

        consumer.setAcknowledgementCommitCallback(callback);
        // As we have already set the callback, we should not add another event. We only add when we initially register.
        verify(applicationEventHandler, times(1)).add(any(ShareAcknowledgementCommitCallbackRegistrationEvent.class));
    }

    @Test
    public void testAcknowledgementCommitCallbackRegistrationEvent_Null() {
        consumer = newConsumer();
        AcknowledgementCommitCallback callback = mock(AcknowledgementCommitCallback.class);

        consumer.setAcknowledgementCommitCallback(null);
        // Initially callback is set to null, setting again to null should not add an event.
        verify(applicationEventHandler, times(0)).add(any(ShareAcknowledgementCommitCallbackRegistrationEvent.class));

        consumer.setAcknowledgementCommitCallback(callback);
        verify(applicationEventHandler, times(1)).add(any(ShareAcknowledgementCommitCallbackRegistrationEvent.class));

        // Now we are changing from a non-null callback to null, this should add an event.
        consumer.setAcknowledgementCommitCallback(null);
        verify(applicationEventHandler).add(argThat(event ->
                event instanceof ShareAcknowledgementCommitCallbackRegistrationEvent &&
                !((ShareAcknowledgementCommitCallbackRegistrationEvent) event).isCallbackRegistered()));
    }

    @Test
    public void testCompleteQuietly() {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CompletableFuture<Object> future = CompletableFuture.completedFuture(null);
        consumer = newConsumer();
        assertDoesNotThrow(() -> consumer.completeQuietly(() ->
                future.get(0, TimeUnit.MILLISECONDS), "test", exception));
        assertNull(exception.get());

        assertDoesNotThrow(() -> consumer.completeQuietly(() -> {
            throw new KafkaException("Test exception");
        }, "test", exception));
        assertInstanceOf(KafkaException.class, exception.get());
    }

    @Test
    public void testSubscribeGeneratesEvent() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        String topic = "topic1";
        final List<String> subscriptionTopic = singletonList(topic);
        completeShareSubscriptionChangeApplicationEventSuccessfully(subscriptions, subscriptionTopic);
        consumer.subscribe(subscriptionTopic);
        assertEquals(singleton(topic), consumer.subscription());
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ShareSubscriptionChangeEvent.class));
    }

    @Test
    public void testUnsubscribeGeneratesUnsubscribeEvent() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);

        consumer.unsubscribe();

        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ShareUnsubscribeEvent.class));
    }

    @Test
    public void testSubscribeToEmptyListActsAsUnsubscribe() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);

        consumer.subscribe(Collections.emptyList());

        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ShareUnsubscribeEvent.class));
    }

    @Test
    public void testSubscribeToNullTopicCollection() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        consumer = newConsumer();
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(emptyTopic)));
    }

    @Test
    public void testBackgroundError() {
        final String groupId = "shareGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        consumer = newConsumer(config);

        final KafkaException expectedException = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorBackgroundEvent = new ErrorEvent(expectedException);
        backgroundEventQueue.add(errorBackgroundEvent);
        consumer.subscribe(Collections.singletonList("t1"));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException.getMessage(), exception.getMessage());
    }

    @Test
    public void testMultipleBackgroundErrors() {
        final String groupId = "shareGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        consumer = newConsumer(config);

        final KafkaException expectedException1 = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorBackgroundEvent1 = new ErrorEvent(expectedException1);
        backgroundEventQueue.add(errorBackgroundEvent1);
        final KafkaException expectedException2 = new KafkaException("Spam, Spam, Spam");
        final ErrorEvent errorBackgroundEvent2 = new ErrorEvent(expectedException2);
        backgroundEventQueue.add(errorBackgroundEvent2);
        consumer.subscribe(Collections.singletonList("t1"));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException1.getMessage(), exception.getMessage());
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testGroupIdNull() {
        final Properties props = requiredConsumerProperties();
        final ConsumerConfig config = new ConsumerConfig(props);

        final Exception exception = assertThrows(
                KafkaException.class,
                () -> consumer = newConsumer(config)
        );

        assertEquals("Failed to construct Kafka share consumer", exception.getMessage());
    }

    @Test
    public void testGroupIdEmpty() {
        testInvalidGroupId("");
    }

    @Test
    public void testGroupIdOnlyWhitespaces() {
        testInvalidGroupId("       ");
    }

    private void testInvalidGroupId(final String groupId) {
        final Properties props = requiredConsumerPropertiesAndGroupId(groupId);
        final ConsumerConfig config = new ConsumerConfig(props);

        final Exception exception = assertThrows(
                KafkaException.class,
                () -> consumer = newConsumer(config)
        );

        assertEquals("Failed to construct Kafka share consumer", exception.getMessage());
    }

    @Test
    public void testEnsurePollEventSentOnConsumerPoll() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(subscriptions);

        final TopicPartition tp = new TopicPartition("topic", 0);
        final TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), tp);
        final ShareInFlightBatch<String, String> batch = new ShareInFlightBatch<>(tip);
        batch.addRecord(new ConsumerRecord<>("topic", 0, 2, "key1", "value1"));
        final ShareFetch<String, String> fetch = ShareFetch.empty();
        fetch.add(tip, batch);
        doAnswer(invocation -> fetch)
                .when(fetchCollector)
                .collect(Mockito.any(ShareFetchBuffer.class));

        final List<String> subscriptionTopic = singletonList("topic");
        completeShareSubscriptionChangeApplicationEventSuccessfully(subscriptions, subscriptionTopic);
        consumer.subscribe(subscriptionTopic);

        consumer.poll(Duration.ofMillis(100));
        verify(applicationEventHandler).add(any(PollEvent.class));
        verify(applicationEventHandler).addAndGet(any(ShareSubscriptionChangeEvent.class));

        completeShareAcknowledgeOnCloseApplicationEventSuccessfully();
        completeShareUnsubscribeApplicationEventSuccessfully(subscriptions);
        consumer.close();
        verify(applicationEventHandler).addAndGet(any(ShareAcknowledgeOnCloseEvent.class));
    }

    private Properties requiredConsumerPropertiesAndGroupId(final String groupId) {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    private Properties requiredConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        return props;
    }

    /**
     * Tests {@link ShareConsumerImpl#processBackgroundEvents(Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} takes a bit of time to complete, but does within the timeout.
     */
    @Test
    public void testProcessBackgroundEventsWithInitialDelay() throws Exception {
        consumer = newConsumer();
        Time time = new MockTime();
        Timer timer = time.timer(1000);
        CompletableFuture<?> future = mock(CompletableFuture.class);
        CountDownLatch latch = new CountDownLatch(3);

        // Mock our call to Future.get(timeout) so that it mimics a delay of 200 milliseconds. Keep in mind that
        // the incremental timeout inside processBackgroundEvents is 100 milliseconds for each pass. Our first two passes
        // will exceed the incremental timeout, but the third will return.
        doAnswer(invocation -> {
            latch.countDown();

            if (latch.getCount() > 0) {
                long timeout = invocation.getArgument(0, Long.class);
                timer.sleep(timeout);
                throw new java.util.concurrent.TimeoutException("Intentional timeout");
            }

            future.complete(null);
            return null;
        }).when(future).get(any(Long.class), any(TimeUnit.class));

        consumer.processBackgroundEvents(future, timer);

        // 800 is the 1000 ms timeout (above) minus the 200 ms delay for the two incremental timeouts/retries.
        assertEquals(800, timer.remainingMs());
    }

    /**
     * Tests {@link ShareConsumerImpl#processBackgroundEvents(Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} is already complete when invoked, so it doesn't have to wait.
     */
    @Test
    public void testProcessBackgroundEventsWithoutDelay() {
        consumer = newConsumer();
        Time time = new MockTime();
        Timer timer = time.timer(1000);

        // Create a future that is already completed.
        CompletableFuture<?> future = CompletableFuture.completedFuture(null);

        consumer.processBackgroundEvents(future, timer);

        // Because we didn't need to perform a timed get, we should still have every last millisecond
        // of our initial timeout.
        assertEquals(1000, timer.remainingMs());
    }

    /**
     * Tests {@link ShareConsumerImpl#processBackgroundEvents(Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} does not complete within the timeout.
     */
    @Test
    public void testProcessBackgroundEventsTimesOut() throws Exception {
        consumer = newConsumer();
        Time time = new MockTime();
        Timer timer = time.timer(1000);
        CompletableFuture<?> future = mock(CompletableFuture.class);

        doAnswer(invocation -> {
            long timeout = invocation.getArgument(0, Long.class);
            timer.sleep(timeout);
            throw new java.util.concurrent.TimeoutException("Intentional timeout");
        }).when(future).get(any(Long.class), any(TimeUnit.class));

        assertThrows(TimeoutException.class, () -> consumer.processBackgroundEvents(future, timer));

        // Because we forced our mocked future to continuously time out, we should have no time remaining.
        assertEquals(0, timer.remainingMs());
    }

    private void completeShareSubscriptionChangeApplicationEventSuccessfully(SubscriptionState subscriptions, List<String> topics) {
        doAnswer(invocation -> {
            ShareSubscriptionChangeEvent event = invocation.getArgument(0);
            subscriptions.subscribeToShareGroup(new HashSet<>(topics));
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ShareSubscriptionChangeEvent.class));
    }

    private void completeShareUnsubscribeApplicationEventSuccessfully(SubscriptionState subscriptions) {
        doAnswer(invocation -> {
            ShareUnsubscribeEvent event = invocation.getArgument(0);
            subscriptions.unsubscribe();
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(ShareUnsubscribeEvent.class));
    }

    private void completeShareAcknowledgeOnCloseApplicationEventSuccessfully() {
        doAnswer(invocation -> {
            ShareAcknowledgeOnCloseEvent event = invocation.getArgument(0);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ShareAcknowledgeOnCloseEvent.class));
    }
}
