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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.RaftMessage;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingMessageQueueTest {

    @Test
    public void testOfferAndPoll() {
        var queue = new BlockingMessageQueue();
        assertTrue(queue.isEmpty());
        assertEquals(Optional.empty(), queue.poll(0));

        var mockMessage1 = Mockito.mock(RaftMessage.class);
        queue.add(mockMessage1);
        assertFalse(queue.isEmpty());
        var entry1 = queue.poll(0);
        assertTrue(entry1.isPresent());
        assertEquals(mockMessage1, entry1.get().message());
        assertTrue(queue.isEmpty());

        var mockMessage2 = Mockito.mock(RaftMessage.class);
        var mockMessage3 = Mockito.mock(RaftMessage.class);
        queue.add(mockMessage2);
        queue.add(mockMessage3);
        assertFalse(queue.isEmpty());
        var entry2 = queue.poll(0);
        var entry3 = queue.poll(0);
        assertTrue(entry2.isPresent());
        assertTrue(entry3.isPresent());
        assertEquals(mockMessage2, entry2.get().message());
        assertEquals(mockMessage3, entry3.get().message());

    }

    @Test
    public void testWakeupFromPoll() {
        var queue = new BlockingMessageQueue();
        queue.wakeup();
        assertEquals(Optional.empty(), queue.poll(Long.MAX_VALUE));
    }

    @Test
    public void testWakeupsAreTransparentToIsEmptyAndDrainedOnPoll() {
        var queue = new BlockingMessageQueue();

        // Wakeups alone should not affect isEmpty
        queue.wakeup();
        queue.wakeup();
        assertTrue(queue.isEmpty());

        // Adding a real message makes the queue non-empty
        var mockMessage = Mockito.mock(RaftMessage.class);
        queue.add(mockMessage);
        assertFalse(queue.isEmpty());

        // Poll should drain all wakeups and return the message in one call
        var entry = queue.poll(0);
        assertTrue(entry.isPresent());
        assertEquals(mockMessage, entry.get().message());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testAddRejectsNullMessage() {
        var queue = new BlockingMessageQueue();

        // Null message should be rejected
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> queue.add(null)
        );
        assertTrue(exception.getMessage().contains("message cannot be null"));
        assertTrue(queue.isEmpty());

        // Valid message should succeed
        var validMessage = Mockito.mock(RaftMessage.class);
        queue.add(validMessage);
        assertFalse(queue.isEmpty());
        var entry = queue.poll(0);
        assertTrue(entry.isPresent());
        assertEquals(validMessage, entry.get().message());
        assertTrue(queue.isEmpty());
    }
}
