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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
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

    @BeforeEach
    public void setup() {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPollAndAdd() {
        EventHandler handler = new DefaultEventHandler(
                new ConsumerConfig(properties),
                new LogContext());
        assertTrue(handler.isEmpty());
        assertTrue(!handler.poll().isPresent());
        handler.add(new StubbedApplicationEvent());
        assertTrue(handler.isEmpty());
    }

    @Test
    public void testRunOnceBackgroundThread() {
        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        EventHandler eventHandler = new DefaultEventHandler(
                new RunOnceBackgroundThread(applicationEventQueue, backgroundEventQueue),
                applicationEventQueue,
                backgroundEventQueue);
        assertTrue(eventHandler.add(new StubbedApplicationEvent("hello-world")));
        assertFalse(eventHandler.isEmpty());
        Optional<BackgroundEvent> event = eventHandler.poll();
        assertTrue(event.isPresent());
        assertTrue(event.get() instanceof RunOnceBackgroundThread.NoopBackgroundEvent);
        assertEquals(BackgroundEvent.EventType.NOOP, event.get().type);
        assertEquals("hello-world", ((RunOnceBackgroundThread.NoopBackgroundEvent) event.get()).message);
    }

    private class StubbedApplicationEvent extends ApplicationEvent {
        public final String message;
        public StubbedApplicationEvent() {
            this("");
        }

        public StubbedApplicationEvent(String message) {
            super(EventType.NOOP, false);
            this.message = message;
        }
    }

    private class RunOnceBackgroundThread implements EventProcessor {
        private BlockingQueue<ApplicationEvent> applicationEventQueue;
        private BlockingQueue<BackgroundEvent> backgroundEventQueue;
        public RunOnceBackgroundThread(BlockingQueue<ApplicationEvent> applicationEvents,
                                       BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            this.applicationEventQueue = applicationEvents;
            this.backgroundEventQueue = backgroundEventQueue;
        }
        @Override
        public void run() {
            while (applicationEventQueue.isEmpty()) { }
            ApplicationEvent event = applicationEventQueue.poll();
            String message = ((StubbedApplicationEvent) event).message;
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
