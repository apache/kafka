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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class PunctuationQueueTest {

    @Test
    public void testPunctuationInterval() {
        final TestProcessor processor = new TestProcessor();
        final ProcessorNode<String, String> node = new ProcessorNode<>("test", processor, null);
        final PunctuationQueue queue = new PunctuationQueue();
        final Punctuator punctuator = new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                node.processor().punctuate(timestamp);
            }
        };

        final PunctuationSchedule sched = new PunctuationSchedule(node, 100L, punctuator);
        final long now = sched.timestamp - 100L;

        queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            @Override
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, processor.punctuatedAt.size());

        queue.mayPunctuate(now + 99L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(0, processor.punctuatedAt.size());

        queue.mayPunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, processor.punctuatedAt.size());

        queue.mayPunctuate(now + 199L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(1, processor.punctuatedAt.size());

        queue.mayPunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        assertEquals(2, processor.punctuatedAt.size());
    }

    private static class TestProcessor extends AbstractProcessor<String, String> {

        public final ArrayList<Long> punctuatedAt = new ArrayList<>();

        @Override
        public void init(ProcessorContext context) {
        }

        @Override
        public void process(String key, String value) {
        }

        @Override
        public void punctuate(long streamTime) {
            punctuatedAt.add(streamTime);
        }

        @Override
        public void close() {
        }
    }

}
