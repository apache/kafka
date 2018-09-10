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
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class KStreamFlatTransformTest extends EasyMockSupport {

    private Number inputKey;
    private Number inputValue;

    private Transformer<Number, Number, Iterable<KeyValue<Integer, Integer>>> transformer;
    private ProcessorContext context;

    private KStreamFlatTransformProcessor<Number, Number, Integer, Integer> processor;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        inputKey = 1;
        inputValue = 10;
        transformer = mock(Transformer.class);
        context = strictMock(ProcessorContext.class);
        processor = new KStreamFlatTransformProcessor<Number, Number, Integer, Integer>(transformer);
    }

    @Test
    public void shouldInitialiseFlatTransformProcessor() {
        transformer.init(context);
        replayAll();

        processor.init(context);

        verifyAll();
    }

    @Test
    public void shouldTransformInputRecordToMultipleOutputRecords() {
        final List<KeyValue<Integer, Integer>> outputRecords = Arrays.asList(
                KeyValue.pair(2, 20),
                KeyValue.pair(3, 30),
                KeyValue.pair(4, 40));
        processor.init(context);
        EasyMock.reset(transformer);

        EasyMock.expect(transformer.transform(inputKey, inputValue)).andReturn(outputRecords);
        context.forward(outputRecords.get(0).key, outputRecords.get(0).value);
        context.forward(outputRecords.get(1).key, outputRecords.get(1).value);
        context.forward(outputRecords.get(2).key, outputRecords.get(2).value);
        replayAll();

        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldTransformInputRecordToEmptyList() {
        processor.init(context);
        EasyMock.reset(transformer);

        EasyMock.expect(transformer.transform(inputKey, inputValue))
            .andReturn(Collections.<KeyValue<Integer, Integer>>emptyList());
        replayAll();

        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldCloseFlatTransformProcessor() {
        transformer.close();
        replayAll();

        processor.close();

        verifyAll();
    }

    @Test
    public void shouldNotAllowTransformInputRecordToNull() {
        processor.init(context);
        EasyMock.reset(transformer);
        EasyMock.expect(transformer.transform(inputKey, inputValue)).andReturn(null);
        replayAll();

        exception.expect(NullPointerException.class);
        exception.expectMessage("result of transform can't be null");
        processor.process(inputKey, inputValue);

        verifyAll();
    }

    @Test
    public void shouldGetFlatTransformProcessor() {
        final TransformerSupplier<Number, Number, Iterable<KeyValue<Integer, Integer>>> transformerSupplier =
            mock(TransformerSupplier.class);
        final KStreamFlatTransform<Number, Number, Integer, Integer> processorSupplier =
            new KStreamFlatTransform<>(transformerSupplier);

        EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
        replayAll();

        final Processor<Number, Number> processor = processorSupplier.get();

        verifyAll();
        assertTrue(processor instanceof KStreamFlatTransformProcessor);
    }
}