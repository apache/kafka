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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.internals.KStreamFlatTransform.KStreamFlatTransformProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KStreamFlatTransformTest {

    private Number inputKey;
    private Number inputValue;

    @Mock
    private Transformer<Number, Number, Iterable<KeyValue<Integer, Integer>>> transformer;
    @Mock
    private InternalProcessorContext<Integer, Integer> context;
    private InOrder inOrder;

    private KStreamFlatTransformProcessor<Number, Number, Integer, Integer> processor;

    @Before
    public void setUp() {
        inputKey = 1;
        inputValue = 10;
        inOrder = inOrder(context);
        processor = new KStreamFlatTransformProcessor<>(transformer);
    }

    @Test
    public void shouldInitialiseFlatTransformProcessor() {
        processor.init(context);

        verify(transformer).init(context);
    }

    @Test
    public void shouldTransformInputRecordToMultipleOutputRecords() {
        final Iterable<KeyValue<Integer, Integer>> outputRecords = Arrays.asList(
                KeyValue.pair(2, 20),
                KeyValue.pair(3, 30),
                KeyValue.pair(4, 40));

        processor.init(context);

        when(transformer.transform(inputKey, inputValue)).thenReturn(outputRecords);

        processor.process(new Record<>(inputKey, inputValue, 0L));

        for (final KeyValue<Integer, Integer> outputRecord : outputRecords) {
            inOrder.verify(context).forward(new Record<>(outputRecord.key, outputRecord.value, 0L));
        }
    }

    @Test
    public void shouldAllowEmptyListAsResultOfTransform() {
        processor.init(context);

        when(transformer.transform(inputKey, inputValue)).thenReturn(Collections.emptyList());

        processor.process(new Record<>(inputKey, inputValue, 0L));

        inOrder.verify(context, never()).forward(ArgumentMatchers.<Record<Integer, Integer>>any());
    }

    @Test
    public void shouldAllowNullAsResultOfTransform() {
        processor.init(context);

        when(transformer.transform(inputKey, inputValue)).thenReturn(null);

        processor.process(new Record<>(inputKey, inputValue, 0L));

        inOrder.verify(context, never()).forward(ArgumentMatchers.<Record<Integer, Integer>>any());
    }

    @Test
    public void shouldCloseFlatTransformProcessor() {
        processor.close();

        verify(transformer).close();
    }

    @Test
    public void shouldGetFlatTransformProcessor() {
        @SuppressWarnings("unchecked")
        final TransformerSupplier<Number, Number, Iterable<KeyValue<Integer, Integer>>> transformerSupplier =
            mock(TransformerSupplier.class);
        final KStreamFlatTransform<Number, Number, Integer, Integer> processorSupplier =
            new KStreamFlatTransform<>(transformerSupplier);

        when(transformerSupplier.get()).thenReturn(transformer);

        final Processor<Number, Number, Integer, Integer> processor = processorSupplier.get();

        assertInstanceOf(KStreamFlatTransformProcessor.class, processor);
    }
}
