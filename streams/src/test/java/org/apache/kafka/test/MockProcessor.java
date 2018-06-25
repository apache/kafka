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
package org.apache.kafka.test;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MockProcessor<K, V> extends AbstractProcessor<K, V> {

    public final ArrayList<String> processed = new ArrayList<>();
    public final ArrayList<K> processedKeys = new ArrayList<>();
    public final ArrayList<V> processedValues = new ArrayList<>();

    public final ArrayList<Long> punctuatedStreamTime = new ArrayList<>();
    public final ArrayList<Long> punctuatedSystemTime = new ArrayList<>();

    public Cancellable scheduleCancellable;

    private final PunctuationType punctuationType;
    private final long scheduleInterval;

    public MockProcessor(final PunctuationType punctuationType, final long scheduleInterval) {
        this.punctuationType = punctuationType;
        this.scheduleInterval = scheduleInterval;
    }

    public MockProcessor() {
        this(PunctuationType.STREAM_TIME, -1);
    }

    @Override
    public void init(final ProcessorContext context) {
        super.init(context);
        if (scheduleInterval > 0L) {
            scheduleCancellable = context.schedule(scheduleInterval, punctuationType, new Punctuator() {
                @Override
                public void punctuate(final long timestamp) {
                    if (punctuationType == PunctuationType.STREAM_TIME) {
                        assertEquals(timestamp, context().timestamp());
                    }
                    assertEquals(-1, context().partition());
                    assertEquals(-1L, context().offset());

                    (punctuationType == PunctuationType.STREAM_TIME ? punctuatedStreamTime : punctuatedSystemTime)
                            .add(timestamp);
                }
            });
        }
    }

    @Override
    public void process(final K key, final V value) {
        processedKeys.add(key);
        processedValues.add(value);
        processed.add((key == null ? "null" : key) + ":" +
                (value == null ? "null" : value));

    }

    public void checkAndClearProcessResult(final String... expected) {
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

    public void checkAndClearPunctuateResult(final PunctuationType type, final long... expected) {
        final ArrayList<Long> punctuated = type == PunctuationType.STREAM_TIME ? punctuatedStreamTime : punctuatedSystemTime;
        assertEquals("the number of outputs:", expected.length, punctuated.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals("output[" + i + "]:", expected[i], (long) punctuated.get(i));
        }

        processed.clear();
    }
}
