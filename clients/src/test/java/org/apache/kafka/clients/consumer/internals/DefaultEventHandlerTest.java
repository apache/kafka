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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultEventHandlerTest {
    private final Properties properties = new Properties();
    private SubscriptionState subscriptions;


    @BeforeEach
    public void setup() {
        subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testBasicPollAndAddWithNoopEvent() {
        LogContext logContext = new LogContext();
        Time time = new MockTime();
        ConsumerMetadata metadata = newConsumerMetadata(false);
        MockClient client = new MockClient(time, metadata);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, 100);
        DefaultEventHandler handler = new DefaultEventHandler(
                time,
                new ConsumerConfig(properties),
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                subscriptions,
                metadata,
                consumerClient);
        assertTrue(client.active());
        assertTrue(handler.isEmpty());
        assertTrue(!handler.poll().isPresent());
        handler.add(new NoopTestApplicationEvent());
        assertTrue(handler.isEmpty());
        assertFalse(client.hasInFlightRequests()); // noop does not send network request
        handler.close();
        assertFalse(client.active());
    }

    @Test
    public void testRunOnceBackgroundThread() {
        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        EventHandler eventHandler = new DefaultEventHandler(
                new RunOnceBackgroundThreadRunnable(applicationEventQueue, backgroundEventQueue),
                applicationEventQueue,
                backgroundEventQueue);
        assertTrue(eventHandler.add(new NoopTestApplicationEvent("hello-world")));
        assertFalse(eventHandler.isEmpty());
        Optional<BackgroundEvent> event = eventHandler.poll();
        assertTrue(event.isPresent());
        assertTrue(event.get() instanceof RunOnceBackgroundThreadRunnable.NoopBackgroundEvent);
        assertEquals(BackgroundEvent.EventType.NOOP, event.get().type);
        assertEquals("hello-world", ((RunOnceBackgroundThreadRunnable.NoopBackgroundEvent) event.get()).message);
    }

    private class NoopTestApplicationEvent extends ApplicationEvent {
        public final String message;
        public NoopTestApplicationEvent() {
            this("");
        }

        public NoopTestApplicationEvent(String message) {
            super(EventType.NOOP, false);
            this.message = message;
        }
    }

    private ConsumerMetadata newConsumerMetadata(boolean includeInternalTopics) {
        long refreshBackoffMs = 50;
        long expireMs = 50000;
        return new ConsumerMetadata(refreshBackoffMs, expireMs, includeInternalTopics, false,
                subscriptions, new LogContext(), new ClusterResourceListeners());
    }

    private class RunOnceBackgroundThreadRunnable implements BackgroundThreadRunnable {
        private BlockingQueue<ApplicationEvent> applicationEventQueue;
        private BlockingQueue<BackgroundEvent> backgroundEventQueue;
        public RunOnceBackgroundThreadRunnable(BlockingQueue<ApplicationEvent> applicationEvents,
                                               BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            this.applicationEventQueue = applicationEvents;
            this.backgroundEventQueue = backgroundEventQueue;
        }
        @Override
        public void run() {
            while (applicationEventQueue.isEmpty()) { }
            ApplicationEvent event = applicationEventQueue.poll();
            String message = ((NoopTestApplicationEvent) event).message;
            backgroundEventQueue.add(new NoopBackgroundEvent(message));
        }

        @Override
        public void close() {
        }

        class NoopBackgroundEvent extends BackgroundEvent {
            public String message;
            public NoopBackgroundEvent(String message) {
                super(EventType.NOOP);
                this.message = message;
            }

            @Override
            public String toString() {
                return type + ":" + message;
            }
        }
    }
}
