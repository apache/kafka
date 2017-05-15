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
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class PunctuationQueueTest {

    private long timestamp;

    @Test
    public void testPunctuationInterval() {
        TestProcessor processor = new TestProcessor();
        final ProcessorNode<String, String> node = new ProcessorNode<>("test", processor, null);
        PunctuationQueue queue = new PunctuationQueue();

        ProcessorNodePunctuator punctuator = new ProcessorNodePunctuator() {
            public void punctuate(ProcessorNode node, long time, Runnable punctuateDelegate) {
                punctuateDelegate.run();
            }
        };

        Runnable punctuateDelegate = new Runnable() {
            @Override
            public void run () {
                node.processor().punctuate(getTimestamp());
            }
        };

        PunctuationSchedule sched = new PunctuationSchedule(node, 100L, punctuateDelegate);
        final long now = sched.timestamp - 100L;

        queue.schedule(sched);

        queue.mayPunctuate(atTimestamp(now), punctuator);
        assertEquals(0, processor.punctuatedAt.size());

        queue.mayPunctuate(atTimestamp(now + 99L), punctuator);
        assertEquals(0, processor.punctuatedAt.size());

        queue.mayPunctuate(atTimestamp(now + 100L), punctuator);
        assertEquals(1, processor.punctuatedAt.size());

        queue.mayPunctuate(atTimestamp(now + 199L), punctuator);
        assertEquals(1, processor.punctuatedAt.size());

        queue.mayPunctuate(atTimestamp(now + 200L), punctuator);
        assertEquals(2, processor.punctuatedAt.size());
    }

    private long getTimestamp () {
        return timestamp;
    }

    private long atTimestamp(long timestamp) {
        return this.timestamp = timestamp;
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
