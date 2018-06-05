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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.test.MockProcessorNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PunctuationQueueTest {

    private final MockProcessorNode<String, String> node = new MockProcessorNode<>();
    private final PunctuationQueue queue = new PunctuationQueue();
    private final Punctuator punctuator = new Punctuator() {
        @Override
        public void punctuate(final long timestamp) {
            node.mockProcessor.punctuatedStreamTime.add(timestamp);
        }
    };

    @Test
    public void testPunctuationInterval() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        final long now = sched.timestamp - 100L;

        queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            @Override
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 99L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 199L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(2, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1001L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1002L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(4, node.mockProcessor.punctuatedStreamTime.size());
    }

    @Test
    public void testPunctuationIntervalCustomAlignment() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 50L, 100L, punctuator);
        final long now = sched.timestamp - 50L;

        queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            @Override
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 49L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 50L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 149L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(2, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1051L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1052L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(3, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 1150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(4, node.mockProcessor.punctuatedStreamTime.size());
    }

    @Test
    public void testPunctuationIntervalCancelFromPunctuator() {
        final PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        final long now = sched.timestamp - 100L;

        final Cancellable cancellable = queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            @Override
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
                // simulate scheduler cancelled from within punctuator
                cancellable.cancel();
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());

        queue.mayPunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, node.mockProcessor.punctuatedStreamTime.size());
    }

    private static class TestProcessor extends AbstractProcessor<String, String> {

        @Override
        public void init(ProcessorContext context) {}

        @Override
        public void process(String key, String value) {}

        @Override
        public void close() {}
    }

}
