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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.deferred.DeferredEvent;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeferredEventCollectionTest {

    private static final Logger LOG = new LogContext().logger(DeferredEventCollectionTest.class);

    @Test
    public void testAddAndSize() {
        DeferredEventCollection collection = new DeferredEventCollection(LOG);
        assertEquals(0, collection.size());

        assertTrue(collection.add(t -> { }));
        assertEquals(1, collection.size());

        assertTrue(collection.add(t -> { }));
        assertEquals(2, collection.size());
    }

    @Test
    public void testCompleteCallsAllEvents() {
        List<Throwable> completedWith = new ArrayList<>();

        DeferredEventCollection collection = new DeferredEventCollection(LOG);
        collection.add(completedWith::add);
        collection.add(completedWith::add);
        collection.add(completedWith::add);

        collection.complete(null);

        assertEquals(3, completedWith.size());
        for (Throwable t : completedWith) {
            assertEquals(null, t);
        }
    }

    @Test
    public void testCompleteWithException() {
        List<Throwable> completedWith = new ArrayList<>();
        RuntimeException exception = new RuntimeException("test exception");

        DeferredEventCollection collection = new DeferredEventCollection(LOG);
        collection.add(completedWith::add);
        collection.add(completedWith::add);

        collection.complete(exception);

        assertEquals(2, completedWith.size());
        for (Throwable t : completedWith) {
            assertEquals(exception, t);
        }
    }

    @Test
    public void testCompleteContinuesOnEventFailure() {
        List<Throwable> completedWith = new ArrayList<>();

        DeferredEventCollection collection = new DeferredEventCollection(LOG);
        collection.add(completedWith::add);
        collection.add(t -> {
            throw new RuntimeException("event failure");
        });
        collection.add(completedWith::add);

        // Should not throw, and should complete all events
        collection.complete(null);

        // The first and third events should have been completed
        assertEquals(2, completedWith.size());
    }

    @Test
    public void testOfFactoryMethod() {
        DeferredEvent event1 = t -> { };
        DeferredEvent event2 = t -> { };

        DeferredEventCollection collection = DeferredEventCollection.of(LOG, event1, event2);

        assertEquals(2, collection.size());
    }

    @Test
    public void testOfFactoryMethodEmpty() {
        DeferredEventCollection collection = DeferredEventCollection.of(LOG);
        assertEquals(0, collection.size());
    }
}
