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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopBackgroundEvent;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ErrorEventHandlerTest {

    private ConsumerTestBuilder.DefaultEventHandlerTestBuilder testBuilder;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private ErrorEventHandler errorEventHandler;
    private BackgroundEventProcessor backgroundEventProcessor;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.DefaultEventHandlerTestBuilder();
        backgroundEventQueue = testBuilder.backgroundEventQueue;
        errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
        backgroundEventProcessor = new BackgroundEventProcessor(testBuilder.logContext, backgroundEventQueue);
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testNoEvents() {
        assertTrue(backgroundEventQueue.isEmpty());
        backgroundEventProcessor.process();
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleEvent() {
        BackgroundEvent event = new NoopBackgroundEvent("A");
        backgroundEventQueue.add(event);
        assertPeeked(event);
        backgroundEventProcessor.process();
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleErrorEvent() {
        KafkaException error = new KafkaException("error");
        BackgroundEvent event = new ErrorBackgroundEvent(error);
        errorEventHandler.handle(error);
        assertPeeked(event);
        assertThrows(error);
    }

    @Test
    public void testInvalidEvent() {
        String message = "I'm a naughty error!";
        BackgroundEvent event = new BackgroundEvent(BackgroundEvent.Type.NOOP) {
            @Override
            public Type type() {
                return null;
            }

            @Override
            public String toString() {
                return message;
            }
        };

        backgroundEventQueue.add(event);
        assertPeeked(event);

        Exception error = new NullPointerException(String.format("Attempt to process BackgroundEvent with null type: %s", message));
        assertThrows(error);
    }

    @Test
    public void testMultipleEvents() {
        BackgroundEvent event1 = new NoopBackgroundEvent("A");
        backgroundEventQueue.add(event1);
        backgroundEventQueue.add(new NoopBackgroundEvent("B"));
        backgroundEventQueue.add(new NoopBackgroundEvent("C"));

        assertPeeked(event1);
        backgroundEventProcessor.process();
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testMultipleErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        errorEventHandler.handle(error1);
        errorEventHandler.handle(error2);
        errorEventHandler.handle(error3);

        assertThrows(new RuntimeException(error1));
    }

    @Test
    public void testMixedEventsWithErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        backgroundEventQueue.add(new NoopBackgroundEvent("A"));
        errorEventHandler.handle(error1);
        backgroundEventQueue.add(new NoopBackgroundEvent("B"));
        errorEventHandler.handle(error2);
        backgroundEventQueue.add(new NoopBackgroundEvent("C"));
        errorEventHandler.handle(error3);
        backgroundEventQueue.add(new NoopBackgroundEvent("D"));

        assertThrows(new RuntimeException(error1));
    }

    private void assertPeeked(BackgroundEvent event) {
        BackgroundEvent peekEvent = backgroundEventQueue.peek();
        assertNotNull(peekEvent);
        assertEquals(event, peekEvent);
    }

    private void assertThrows(Throwable error) {
        assertFalse(backgroundEventQueue.isEmpty());

        try {
            backgroundEventProcessor.process();
            fail("Should have thrown error: " + error);
        } catch (Throwable t) {
            assertEquals(error.getClass(), t.getClass());
            assertEquals(error.getMessage(), t.getMessage());
        }

        assertTrue(backgroundEventQueue.isEmpty());
    }
}
