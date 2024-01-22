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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.test.MockProcessorNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PunctuationQueueTest {

    private final MockProcessorNode<String, String, ?, ?> node = new MockProcessorNode<>();
    private final PunctuationQueue queue = new PunctuationQueue();
    private final Punctuator punctuator = timestamp -> node.mockProcessor.punctuatedStreamTime().add(timestamp);

    @Test
    public void testPunctuationInterval() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        final long now = sched.timestamp - 100L;

        queue.schedule(sched);
        assertCanPunctuateAtPrecisely(now + 100L);

        final ProcessorNodePunctuator processorNodePunctuator = (node, timestamp, type, punctuator) -> punctuator.punctuate(timestamp);

        queue.maybePunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 100L);

        queue.maybePunctuate(now + 99L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 100L);

        queue.maybePunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 200L);

        queue.maybePunctuate(now + 199L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 200L);

        queue.maybePunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(2, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 300L);

        queue.maybePunctuate(now + 1001L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1100L);

        queue.maybePunctuate(now + 1002L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1100L);

        queue.maybePunctuate(now + 1100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(4, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1200L);
    }

    @Test
    public void testPunctuationIntervalCustomAlignment() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 50L, 100L, punctuator);
        final long now = sched.timestamp - 50L;

        queue.schedule(sched);
        assertCanPunctuateAtPrecisely(now + 50L);

        final ProcessorNodePunctuator processorNodePunctuator =
            (node, timestamp, type, punctuator) -> punctuator.punctuate(timestamp);

        queue.maybePunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 50L);

        queue.maybePunctuate(now + 49L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 50L);

        queue.maybePunctuate(now + 50L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 150L);

        queue.maybePunctuate(now + 149L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 150L);

        queue.maybePunctuate(now + 150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(2, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 250L);

        queue.maybePunctuate(now + 1051L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1150L);

        queue.maybePunctuate(now + 1052L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1150L);

        queue.maybePunctuate(now + 1150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(4, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 1250L);
    }

    @Test
    public void testPunctuationIntervalCancelFromPunctuator() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        final long now = sched.timestamp - 100L;

        final Cancellable cancellable = queue.schedule(sched);
        assertCanPunctuateAtPrecisely(now + 100L);

        final ProcessorNodePunctuator processorNodePunctuator = (node, timestamp, type, punctuator) -> {
            punctuator.punctuate(timestamp);
            // simulate scheduler cancelled from within punctuator
            cancellable.cancel();
        };

        queue.maybePunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime().size());
        assertCanPunctuateAtPrecisely(now + 100L);

        queue.maybePunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertFalse(queue.canPunctuate(Long.MAX_VALUE));

        queue.maybePunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime().size());
        assertFalse(queue.canPunctuate(Long.MAX_VALUE));
    }

    private void assertCanPunctuateAtPrecisely(final long now) {
        assertFalse(queue.canPunctuate(now - 1));
        assertTrue(queue.canPunctuate(now));
        assertTrue(queue.canPunctuate(now + 1));
    }

}
