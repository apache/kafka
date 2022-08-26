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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingMessageQueueTest {

    @Test
    public void testOfferAndPoll() {
        BlockingMessageQueue queue = new BlockingMessageQueue();
        assertTrue(queue.isEmpty());
        assertNull(queue.poll(0));

        RaftMessage message1 = Mockito.mock(RaftMessage.class);
        queue.add(message1);
        assertFalse(queue.isEmpty());
        assertEquals(message1, queue.poll(0));
        assertTrue(queue.isEmpty());

        RaftMessage message2 = Mockito.mock(RaftMessage.class);
        RaftMessage message3 = Mockito.mock(RaftMessage.class);
        queue.add(message2);
        queue.add(message3);
        assertFalse(queue.isEmpty());
        assertEquals(message2, queue.poll(0));
        assertEquals(message3, queue.poll(0));

    }

    @Test
    public void testWakeupFromPoll() {
        BlockingMessageQueue queue = new BlockingMessageQueue();
        queue.wakeup();
        assertNull(queue.poll(Long.MAX_VALUE));
    }

}