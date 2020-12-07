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

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MockApiProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final long scheduleInterval;
    private final PunctuationType punctuationType;
    private final List<MockApiProcessor<KIn, VIn, KOut, VOut>> processors = new ArrayList<>();

    public MockApiProcessorSupplier() {
        this(-1L);
    }

    public MockApiProcessorSupplier(final long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockApiProcessorSupplier(final long scheduleInterval, final PunctuationType punctuationType) {
        this.scheduleInterval = scheduleInterval;
        this.punctuationType = punctuationType;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        final MockApiProcessor<KIn, VIn, KOut, VOut> processor = new MockApiProcessor<>(punctuationType, scheduleInterval);

        // to keep tests simple, ignore calls from ApiUtils.checkSupplier
        if (!StreamsTestUtils.isCheckSupplierCall()) {
            processors.add(processor);
        }

        return processor;
    }

    // get the captured processor assuming that only one processor gets returned from this supplier
    public MockApiProcessor<KIn, VIn, KOut, VOut> theCapturedProcessor() {
        return capturedProcessors(1).get(0);
    }

    public int capturedProcessorsCount() {
        return processors.size();
    }

    // get the captured processors with the expected number
    public List<MockApiProcessor<KIn, VIn, KOut, VOut>> capturedProcessors(final int expectedNumberOfProcessors) {
        assertEquals(expectedNumberOfProcessors, processors.size());

        return processors;
    }
}
