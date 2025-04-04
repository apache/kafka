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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics.BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BackgroundEventHandlerTest {
    private final BlockingQueue<BackgroundEvent> backgroundEventsQueue =  new LinkedBlockingQueue<>();

    @Test
    public void testRecordBackgroundEventQueueSize() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics)) {
            BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
                backgroundEventsQueue,
                new MockTime(0),
                asyncConsumerMetrics);
            // add event
            backgroundEventHandler.add(new ErrorEvent(new Throwable()));
            assertEquals(
                1,
                (double) metrics.metric(
                    metrics.metricName(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME, CONSUMER_METRIC_GROUP)
                ).metricValue()
            );

            // drain event
            backgroundEventHandler.drainEvents();
            assertEquals(
                0,
                (double) metrics.metric(
                    metrics.metricName(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME, CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
        }
    }
}
