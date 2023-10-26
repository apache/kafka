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

import org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder;
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

public class BackgroundEventHandlerTest {

    private ConsumerTestBuilder testBuilder;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private BackgroundEventHandler backgroundEventHandler;
    private BackgroundEventProcessor backgroundEventProcessor;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder();
        backgroundEventQueue = testBuilder.backgroundEventQueue;
        backgroundEventHandler = testBuilder.backgroundEventHandler;
        backgroundEventProcessor = testBuilder.backgroundEventProcessor;
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testNoEvents() {
        assertTrue(backgroundEventQueue.isEmpty());
        backgroundEventProcessor.process((event, error) -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleEvent() {
        BackgroundEvent event = new ErrorBackgroundEvent(new RuntimeException("A"));
        backgroundEventQueue.add(event);
        assertPeeked(event);
        backgroundEventProcessor.process((e, error) -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleErrorEvent() {
        KafkaException error = new KafkaException("error");
        BackgroundEvent event = new ErrorBackgroundEvent(error);
        backgroundEventHandler.add(new ErrorBackgroundEvent(error));
        assertPeeked(event);
        assertProcessThrows(error);
    }

    @Test
    public void testMultipleEvents() {
        BackgroundEvent event1 = new ErrorBackgroundEvent(new RuntimeException("A"));
        backgroundEventQueue.add(event1);
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("B")));
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("C")));

        assertPeeked(event1);
        backgroundEventProcessor.process((event, error) -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testMultipleErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        backgroundEventHandler.add(new ErrorBackgroundEvent(error1));
        backgroundEventHandler.add(new ErrorBackgroundEvent(error2));
        backgroundEventHandler.add(new ErrorBackgroundEvent(error3));

        assertProcessThrows(new KafkaException(error1));
    }

    @Test
    public void testMixedEventsWithErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        RuntimeException errorToCheck = new RuntimeException("A");
        backgroundEventQueue.add(new ErrorBackgroundEvent(errorToCheck));
        backgroundEventHandler.add(new ErrorBackgroundEvent(error1));
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("B")));
        backgroundEventHandler.add(new ErrorBackgroundEvent(error2));
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("C")));
        backgroundEventHandler.add(new ErrorBackgroundEvent(error3));
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("D")));

        assertProcessThrows(new KafkaException(errorToCheck));
    }

    private void assertPeeked(BackgroundEvent event) {
        BackgroundEvent peekEvent = backgroundEventQueue.peek();
        assertNotNull(peekEvent);
        assertEquals(event, peekEvent);
    }

    private void assertProcessThrows(Throwable error) {
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
