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
package org.apache.kafka.streams.kstream.internals;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.KStreamFlatTransformValues.KStreamFlatTransformValuesProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

public class KStreamFlatTransformValuesTest extends EasyMockSupport {

    private Integer inputKey;
    private Integer inputValue;

    private ValueTransformerWithKey<Integer, Integer, Iterable<String>> valueTransformer;
    private ProcessorContext context;

    private KStreamFlatTransformValuesProcessor<Integer, Integer, String> processor;

    @Before
    public void setUp() {
        inputKey = 1;
        inputValue = 10;
        valueTransformer = mock(ValueTransformerWithKey.class);
        context = strictMock(ProcessorContext.class);
        processor = new KStreamFlatTransformValuesProcessor<>(valueTransformer);
    }

    @Test
    public void shouldInitializeFlatTransformValuesProcessor() {
        valueTransformer.init(EasyMock.isA(ForwardingDisabledProcessorContext.class));
        replayAll();

        processor.init(context);

        verifyAll();
    }

    @Test
    public void shouldTransformInputRecordToMultipleOutputValues() {
        final Iterable<String> outputValues = Arrays.asList(
                "Hello",
                "Blue",
                "Planet");
        processor.init(context);
        EasyMock.reset(valueTransformer);

        EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(outputValues);
        for (final String outputValue : outputValues) {
            context.forward(inputKey, outputValue);
        }
        replayAll();

        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldEmitNoRecordIfTransformReturnsEmptyList() {
        processor.init(context);
        EasyMock.reset(valueTransformer);

        EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(Collections.<String>emptyList());
        replayAll();

        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldEmitNoRecordIfTransformReturnsNull() {
        processor.init(context);
        EasyMock.reset(valueTransformer);

        EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(null);
        replayAll();

        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldCloseFlatTransformValuesProcessor() {
        valueTransformer.close();
        replayAll();

        processor.close();

        verifyAll();
    }

    @Test
    public void shouldGetFlatTransformValuesProcessor() {
        final ValueTransformerWithKeySupplier<Integer, Integer, Iterable<String>> valueTransformerSupplier =
            mock(ValueTransformerWithKeySupplier.class);
        final KStreamFlatTransformValues<Integer, Integer, String> processorSupplier =
            new KStreamFlatTransformValues<>(valueTransformerSupplier);

        EasyMock.expect(valueTransformerSupplier.get()).andReturn(valueTransformer);
        replayAll();

        final Processor<Integer, Integer> processor = processorSupplier.get();

        verifyAll();
        assertTrue(processor instanceof KStreamFlatTransformValuesProcessor);
    }
}
