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
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ApplicationEventHandlerTest {
    private final Time time = new MockTime();
    private final BlockingQueue<ApplicationEvent> applicationEventsQueue =  new LinkedBlockingQueue<>();
    private final ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
    private final NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
    private final RequestManagers requestManagers = mock(RequestManagers.class);
    private final CompletableEventReaper applicationEventReaper = mock(CompletableEventReaper.class);

    @Test
    public void testRecordApplicationEventQueueSize() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics);
             ApplicationEventHandler applicationEventHandler = new ApplicationEventHandler(
                     new LogContext(),
                     time,
                     applicationEventsQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            // add event
            applicationEventHandler.add(new PollEvent(time.milliseconds()));
            assertEquals(
                1,
                (double) metrics.metric(
                    metrics.metricName(
                        AsyncConsumerMetrics.APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME,
                        ConsumerUtils.CONSUMER_METRIC_GROUP
                    )
                ).metricValue()
            );
        }
    }
}
