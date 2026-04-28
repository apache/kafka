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
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
