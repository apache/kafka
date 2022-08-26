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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorAdapter;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

public class MockProcessorNode<KIn, VIn, KOut, VOut> extends ProcessorNode<KIn, VIn, KOut, VOut> {

    private static final String NAME = "MOCK-PROCESS-";
    private static final AtomicInteger INDEX = new AtomicInteger(1);

    public final MockProcessor<KIn, VIn> mockProcessor;

    public boolean closed;
    public boolean initialized;

    public MockProcessorNode(final long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockProcessorNode(final long scheduleInterval, final PunctuationType punctuationType) {
        this(new MockProcessor<>(punctuationType, scheduleInterval));
    }

    public MockProcessorNode() {
        this(new MockProcessor<>());
    }

    @SuppressWarnings("unchecked")
    private MockProcessorNode(final MockProcessor<KIn, VIn> mockProcessor) {
        super(NAME + INDEX.getAndIncrement(), ProcessorAdapter.adapt(mockProcessor), Collections.<String>emptySet());

        this.mockProcessor = mockProcessor;
    }

    @Override
    public void init(final InternalProcessorContext<KOut, VOut> context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        mockProcessor.process(record.key(), record.value());
    }

    @Override
    public void close() {
        super.close();
        this.closed = true;
    }
}