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

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MockProcessorSupplier<K, V> implements ProcessorSupplier<K, V> {

    private final long scheduleInterval;
    private final PunctuationType punctuationType;
    private final List<MockProcessor<K, V>> processors = new ArrayList<>();

    public MockProcessorSupplier() {
        this(-1L);
    }

    public MockProcessorSupplier(final long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockProcessorSupplier(final long scheduleInterval, final PunctuationType punctuationType) {
        this.scheduleInterval = scheduleInterval;
        this.punctuationType = punctuationType;
    }

    @Override
    public Processor<K, V> get() {
        final MockProcessor<K, V> processor = new MockProcessor<>(punctuationType, scheduleInterval);

        // to keep tests simple, ignore calls from ApiUtils.checkSupplier
        if (!StreamsTestUtils.isCheckSupplierCall()) {
            processors.add(processor);
        }

        return processor;
    }

    // get the captured processor assuming that only one processor gets returned from this supplier
    public MockProcessor<K, V> theCapturedProcessor() {
        return capturedProcessors(1).get(0);
    }

    public int capturedProcessorsCount() {
        return processors.size();
    }

        // get the captured processors with the expected number
    public List<MockProcessor<K, V>> capturedProcessors(final int expectedNumberOfProcessors) {
        assertEquals(expectedNumberOfProcessors, processors.size());

        return processors;
    }
}
