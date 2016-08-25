/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MockProcessorSupplier<K, V> implements ProcessorSupplier<K, V> {

    public final ArrayList<String> processed = new ArrayList<>();
    public final ArrayList<Long> punctuated = new ArrayList<>();

    private final long scheduleInterval;

    public MockProcessorSupplier() {
        this(-1L);
    }

    public MockProcessorSupplier(long scheduleInterval) {
        this.scheduleInterval = scheduleInterval;
    }

    @Override
    public Processor<K, V> get() {
        return new MockProcessor();
    }

    public class MockProcessor extends AbstractProcessor<K, V> {

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            if (scheduleInterval > 0L)
                context.schedule(scheduleInterval);
        }

        @Override
        public void process(K key, V value) {
            processed.add((key == null ? "null" : key) + ":" +
                    (value == null ? "null" : value));

        }

        @Override
        public void punctuate(long streamTime) {
            assertEquals(streamTime, context().timestamp());
            assertEquals(-1, context().partition());
            assertEquals(-1L, context().offset());

            punctuated.add(streamTime);
        }
    }

    public void checkAndClearProcessResult(String... expected) {
        assertEquals("the number of outputs:" + processed, expected.length, processed.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals("output[" + i + "]:", expected[i], processed.get(i));
        }

        processed.clear();
    }

    public void checkEmptyAndClearProcessResult() {

        assertEquals("the number of outputs:", 0, processed.size());
        processed.clear();
    }

    public void checkAndClearPunctuateResult(long... expected) {
        assertEquals("the number of outputs:", expected.length, punctuated.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals("output[" + i + "]:", expected[i], (long) punctuated.get(i));
        }

        processed.clear();
    }

}
