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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

public class MockApiFixedKeyProcessorSupplier<KIn, VIn, VOut>
    implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

    private final long scheduleInterval;
    private final PunctuationType punctuationType;
    private final List<MockApiFixedKeyProcessor<KIn, VIn, VOut>> processors = new ArrayList<>();

    public MockApiFixedKeyProcessorSupplier() {
        this(-1L);
    }

    public MockApiFixedKeyProcessorSupplier(final long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockApiFixedKeyProcessorSupplier(final long scheduleInterval, final PunctuationType punctuationType) {
        this.scheduleInterval = scheduleInterval;
        this.punctuationType = punctuationType;
    }

    @Override
    public FixedKeyProcessor<KIn, VIn, VOut> get() {
        final MockApiFixedKeyProcessor<KIn, VIn, VOut> processor = new MockApiFixedKeyProcessor<>(punctuationType, scheduleInterval);

        // to keep tests simple, ignore calls from ApiUtils.checkSupplier
        if (!StreamsTestUtils.isCheckSupplierCall()) {
            processors.add(processor);
        }

        return processor;
    }

    // get the captured processor assuming that only one processor gets returned from this supplier
    public MockApiFixedKeyProcessor<KIn, VIn, VOut> theCapturedProcessor() {
        return capturedProcessors(1).get(0);
    }

    public int capturedProcessorsCount() {
        return processors.size();
    }

    // get the captured processors with the expected number
    public List<MockApiFixedKeyProcessor<KIn, VIn, VOut>> capturedProcessors(final int expectedNumberOfProcessors) {
        assertEquals(expectedNumberOfProcessors, processors.size());

        return processors;
    }
}
