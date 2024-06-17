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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.KStreamFlatTransformValues.KStreamFlatTransformValuesProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KStreamFlatTransformValuesTest {

    private Integer inputKey;
    private Integer inputValue;

    @Mock
    private ValueTransformerWithKey<Integer, Integer, Iterable<String>> valueTransformer;
    @Mock
    private InternalProcessorContext<Integer, String> context;
    private InOrder inOrder;

    private KStreamFlatTransformValuesProcessor<Integer, Integer, String> processor;

    @Before
    public void setUp() {
        inputKey = 1;
        inputValue = 10;
        inOrder = inOrder(context);
        processor = new KStreamFlatTransformValuesProcessor<>(valueTransformer);
    }

    @Test
    public void shouldInitializeFlatTransformValuesProcessor() {
        processor.init(context);

        verify(valueTransformer).init(ArgumentMatchers.isA(ForwardingDisabledProcessorContext.class));
    }

    @Test
    public void shouldTransformInputRecordToMultipleOutputValues() {
        final Iterable<String> outputValues = Arrays.asList(
                "Hello",
                "Blue",
                "Planet");

        processor.init(context);

        when(valueTransformer.transform(inputKey, inputValue)).thenReturn(outputValues);

        processor.process(new Record<>(inputKey, inputValue, 0L));

        for (final String outputValue : outputValues) {
            inOrder.verify(context).forward(new Record<>(inputKey, outputValue, 0L));
        }
    }

    @Test
    public void shouldEmitNoRecordIfTransformReturnsEmptyList() {
        processor.init(context);

        when(valueTransformer.transform(inputKey, inputValue)).thenReturn(Collections.emptyList());

        processor.process(new Record<>(inputKey, inputValue, 0L));

        inOrder.verify(context, never()).forward(ArgumentMatchers.<Record<Integer, String>>any());
    }

    @Test
    public void shouldEmitNoRecordIfTransformReturnsNull() {
        processor.init(context);

        when(valueTransformer.transform(inputKey, inputValue)).thenReturn(null);

        processor.process(new Record<>(inputKey, inputValue, 0L));

        inOrder.verify(context, never()).forward(ArgumentMatchers.<Record<Integer, String>>any());
    }

    @Test
    public void shouldCloseFlatTransformValuesProcessor() {
        processor.close();

        verify(valueTransformer).close();
    }

    @Test
    public void shouldGetFlatTransformValuesProcessor() {
        @SuppressWarnings("unchecked")
        final ValueTransformerWithKeySupplier<Integer, Integer, Iterable<String>> valueTransformerSupplier =
            mock(ValueTransformerWithKeySupplier.class);
        final KStreamFlatTransformValues<Integer, Integer, String> processorSupplier =
            new KStreamFlatTransformValues<>(valueTransformerSupplier);

        when(valueTransformerSupplier.get()).thenReturn(valueTransformer);

        final Processor<Integer, Integer, Integer, String> processor = processorSupplier.get();

        assertInstanceOf(KStreamFlatTransformValuesProcessor.class, processor);
    }
}
