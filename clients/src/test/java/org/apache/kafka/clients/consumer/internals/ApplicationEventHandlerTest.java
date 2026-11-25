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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class ApplicationEventHandlerTest {
    private final Time time = new MockTime();
    private final int initializationTimeoutMs = 50;
    private final BlockingQueue<ApplicationEvent> applicationEventsQueue =  new LinkedBlockingQueue<>();
    private final ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
    private final NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
    private final RequestManagers requestManagers = mock(RequestManagers.class);
    private final CompletableEventReaper applicationEventReaper = mock(CompletableEventReaper.class);

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRecordApplicationEventQueueSize(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = spy(new AsyncConsumerMetrics(metrics, groupName));
             ApplicationEventHandler applicationEventHandler = new ApplicationEventHandler(
                     new LogContext(),
                     time,
                     initializationTimeoutMs,
                     applicationEventsQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            // add event
            applicationEventHandler.add(new AsyncPollEvent(time.milliseconds() + 10, time.milliseconds()));
            verify(asyncConsumerMetrics).recordApplicationEventQueueSize(1);
        }
    }

    @Test
    public void testFailOnInitializeResources() {
        RuntimeException rootFailure = new RuntimeException("root failure");
        KafkaException error = assertInitializeResourcesError(
            KafkaException.class,
            () -> {
                throw rootFailure;
            }
        );
        assertEquals(rootFailure, error.getCause());
    }

    @Test
    public void testDelayInInitializeResources() {
        assertInitializeResourcesError(
            TimeoutException.class,
            () -> {
                long delayMs = initializationTimeoutMs * 2;
                org.apache.kafka.common.utils.Utils.sleep(delayMs);
                return networkClientDelegate;
            }
        );
    }

    @Test
    public void testInterruptInInitializeResources() {
        Thread.currentThread().interrupt();
        assertInitializeResourcesError(InterruptException.class, () -> networkClientDelegate);
    }

    @Test
    public void testAddThrowsWhenBackgroundThreadDead() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = spy(new AsyncConsumerMetrics(metrics, "test-group"));
             ApplicationEventHandler handler = newHandler(time, asyncConsumerMetrics)) {
            handler.close(Duration.ZERO);

            KafkaException error = assertThrows(KafkaException.class,
                () -> handler.add(new AsyncPollEvent(time.milliseconds() + 10, time.milliseconds())));
            assertTrue(error.getMessage().contains("background thread is not running"));
        }
    }

    /**
     * The mocked processor never completes an event, so the only thing that can end the wait for the event's
     * result is the event's deadline. Before that bound existed, the call blocked until the background thread's
     * reaper expired the event, which never happens once the background thread is gone.
     */
    @Test
    public void testAddAndGetTimesOutWhenEventIsNotCompleted() {
        Time systemTime = Time.SYSTEM;
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ApplicationEventHandler handler = newHandler(systemTime, asyncConsumerMetrics)) {
            long deadlineMs = systemTime.milliseconds() + 200;
            UpdatePatternSubscriptionEvent event = new UpdatePatternSubscriptionEvent(deadlineMs);

            long startMs = systemTime.milliseconds();
            TimeoutException error = assertThrows(TimeoutException.class, () -> handler.addAndGet(event));
            long elapsedMs = systemTime.milliseconds() - startMs;

            assertTrue(elapsedMs >= 200,
                    "Expected the wait to last until the deadline, but it lasted " + elapsedMs + " ms");
            assertTrue(error.getMessage().contains(UpdatePatternSubscriptionEvent.class.getSimpleName()),
                    "Expected the error to name the event, but it was: " + error.getMessage());
        }
    }

    /**
     * Reproduces the situation from the reported bug: the application thread is waiting for an event's result and
     * the background thread goes away without completing it. The wait has to end even though the event's deadline
     * has not passed and the event is never completed.
     */
    @Test
    public void testAddAndGetFailsWhenBackgroundThreadStops() throws Exception {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ApplicationEventHandler handler = newHandler(time, asyncConsumerMetrics)) {
            // The deadline never passes within the test, so only the background thread's termination can end the wait.
            CompletableApplicationEvent<Void> event = new UpdatePatternSubscriptionEvent(Long.MAX_VALUE);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Void> result = executor.submit(() -> handler.addAndGet(event));
                // Wait until the background thread has taken the event, which means the caller is now waiting for its
                // result and will never receive it.
                verify(applicationEventProcessor, timeout(5000)).process(event);

                handler.close(Duration.ofSeconds(1));

                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> result.get(5, TimeUnit.SECONDS));
                assertInstanceOf(KafkaException.class, failure.getCause());
                assertTrue(failure.getCause().getMessage().contains("background thread is not running"),
                        "Expected an error about the background thread, but it was: " +
                                failure.getCause().getMessage());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    /**
     * An event that is already past its deadline when the wait starts is still given the chance to return the result
     * it already holds, which keeps calls such as {@code partitionsFor(topic, Duration.ZERO)} serving cached data.
     */
    @Test
    public void testAddAndGetReturnsResultWhenDeadlineAlreadyPassed() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ApplicationEventHandler handler = newHandler(time, asyncConsumerMetrics)) {
            UpdatePatternSubscriptionEvent event = new UpdatePatternSubscriptionEvent(time.milliseconds() - 1000);
            event.future().complete(null);

            assertNull(handler.addAndGet(event));
        }
    }

    /**
     * A result that arrives while the application thread is waiting is returned without waiting for the deadline.
     */
    @Test
    public void testAddAndGetReturnsResultWhenEventCompletes() throws Exception {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ApplicationEventHandler handler = newHandler(time, asyncConsumerMetrics)) {
            CompletableApplicationEvent<Void> event = new UpdatePatternSubscriptionEvent(Long.MAX_VALUE);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Void> result = executor.submit(() -> handler.addAndGet(event));
                event.future().complete(null);

                assertNull(result.get(5, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    /**
     * An event that the background thread already failed with a {@link TimeoutException} (which is what the reaper
     * does for an expired event) is reported to the caller, not waited on again.
     */
    @Test
    public void testAddAndGetPropagatesEventTimeout() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ApplicationEventHandler handler = newHandler(time, asyncConsumerMetrics)) {
            UpdatePatternSubscriptionEvent event = new UpdatePatternSubscriptionEvent(Long.MAX_VALUE);
            event.future().completeExceptionally(new TimeoutException("event expired"));

            TimeoutException error = assertThrows(TimeoutException.class, () -> handler.addAndGet(event));
            assertEquals("event expired", error.getMessage());
        }
    }

    private ApplicationEventHandler newHandler(final Time handlerTime,
                                               final AsyncConsumerMetrics asyncConsumerMetrics) {
        return new ApplicationEventHandler(
                new LogContext(),
                handlerTime,
                initializationTimeoutMs,
                applicationEventsQueue,
                applicationEventReaper,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers,
                asyncConsumerMetrics
        );
    }

    private <T extends Throwable> T assertInitializeResourcesError(Class<T> exceptionClass,
                                                                   Supplier<NetworkClientDelegate> networkClientDelegateSupplier) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = spy(new AsyncConsumerMetrics(metrics, "test-group"))) {
            return assertThrows(exceptionClass, () -> new ApplicationEventHandler(
                new LogContext(),
                time,
                initializationTimeoutMs,
                applicationEventsQueue,
                applicationEventReaper,
                () -> applicationEventProcessor,
                networkClientDelegateSupplier,
                () -> requestManagers,
                asyncConsumerMetrics
            ));
        }
    }
}
