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

package org.apache.kafka.stream;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.stream.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.UnlimitedWindow;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamJoinTest {

    private String topicName = "topic";

    private KStreamTopology topology = new MockKStreamTopology();
    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    private ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(String value1, String value2) {
            return value1 + "+" + value2;
        }
    };

    private ValueMapper<String, String> valueMapper = new ValueMapper<String, String>() {
        @Override
        public String apply(String value) {
            return "#" + value;
        }
    };

    private ValueMapper<String, Iterable<String>> valueMapper2 = new ValueMapper<String, Iterable<String>>() {
        @Override
        public Iterable<String> apply(String value) {
            return (Iterable<String>) Utils.mkSet(value);
        }
    };

    private KeyValueMapper<Integer, String, Integer, String> keyValueMapper =
        new KeyValueMapper<Integer, String, Integer, String>() {
            @Override
            public KeyValue<Integer, String> apply(Integer key, String value) {
                return KeyValue.pair(key, value);
            }
        };

    KeyValueMapper<Integer, String, Integer, Iterable<String>> keyValueMapper2 =
        new KeyValueMapper<Integer, String, Integer, Iterable<String>>() {
            @Override
            public KeyValue<Integer, Iterable<String>> apply(Integer key, String value) {
                return KeyValue.pair(key, (Iterable<String>) Utils.mkSet(value));
            }
        };


    @Test
    public void testJoin() {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> wip
    processor = new TestProcessor<>();
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
    windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());
=======
        final int[] expectedKeys = new int[]{0, 1, 2, 3};
>>>>>>> compile and test passed

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;
        String[] expected;

        processor = new MockProcessor<>();
        stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        windowed1 = stream1.with(new UnlimitedWindow<>());
        windowed2 = stream2.with(new UnlimitedWindow<>());

        windowed1.join(windowed2, joiner).process(processor);

        // push two items to the main stream. the other stream's window is empty

        for (int i = 0; i < 2; i++) {
            ((KStreamSource<Integer, String>) stream1).source().receive(expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(0, processor.processed.size());

        // push two items to the other stream. the main stream's window has two items

        for (int i = 0; i < 2; i++) {
            ((KStreamSource<Integer, String>) stream2).source().receive(expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(2, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

        processor.processed.clear();

        // push all items to the main stream. this should produce two items.

        for (int i = 0; i < expectedKeys.length; i++) {
            ((KStreamSource<Integer, String>) stream1).source().receive(expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(2, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

        processor.processed.clear();

        // there will be previous two items + all items in the main stream's window, thus two are duplicates.

        // push all items to the other stream. this should produce 6 items
        for (int i = 0; i < expectedKeys.length; i++) {
            ((KStreamSource<Integer, String>) stream2).source().receive(expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(6, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

    @Test
    public void testJoinPrior() {

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;
        String[] expected;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
>>>>>>> wip
    processor = new TestProcessor<>();
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
    windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());
=======
        KStreamTopology initializer = new MockKStreamTopology();
        processor = new MockProcessor<>();
        stream1 = new KStreamSource<>(null, initializer);
        stream2 = new KStreamSource<>(null, initializer);
        windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
        windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());
>>>>>>> compile and test passed

        boolean exceptionRaised = false;

        try {
            windowed1.joinPrior(windowed2, joiner).process(processor);

            KStreamContext context = new MockKStreamContext(null, null);
            stream1.bind(context, streamMetadata);
            stream2.bind(context, streamMetadata);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }
=======
        processor = new MockProcessor<>();
        stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        windowed1 = stream1.with(new UnlimitedWindow<>());
        windowed2 = stream2.with(new UnlimitedWindow<>());
>>>>>>> wip

        windowed1.joinPrior(windowed2, joiner).process(processor);

        MockProcessorContext context = new MockProcessorContext(null, null);
        topology.init(context);

        // push two items to the main stream. the other stream's window is empty

        for (int i = 0; i < 2; i++) {
            context.setTime(i);

            ((KStreamSource<Integer, String>) stream1).source().receive(expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(0, processor.processed.size());

        // push two items to the other stream. the main stream's window has two items
        // no corresponding item in the main window has a newer timestamp

        for (int i = 0; i < 2; i++) {
            context.setTime(i + 1);

            ((KStreamSource<Integer, String>) stream2).source().receive(expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(0, processor.processed.size());

        processor.processed.clear();

        // push all items with newer timestamps to the main stream. this should produce two items.

        for (int i = 0; i < expectedKeys.length; i++) {
            context.setTime(i + 2);

            ((KStreamSource<Integer, String>) stream1).source().receive(expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(2, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

        processor.processed.clear();

        // there will be previous two items + all items in the main stream's window, thus two are duplicates.

        // push all items with older timestamps to the other stream. this should produce six items
        for (int i = 0; i < expectedKeys.length; i++) {
            context.setTime(i);

            ((KStreamSource<Integer, String>) stream2).source().receive(expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(6, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
<<<<<<< HEAD

    }

    @Test
    public void testMap() {
        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> mapped1;
        KStream<Integer, String> mapped2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;

        processor = new MockProcessor<>();

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.map(keyValueMapper);
    mapped2 = stream2.map(keyValueMapper);
>>>>>>> new api model
=======
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
>>>>>>> wip
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
=======
        boolean exceptionRaised;
>>>>>>> compile and test passed

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.map(keyValueMapper);
            mapped2 = stream2.map(keyValueMapper);

            exceptionRaised = false;
            windowed1 = stream1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);
            stream1.bind(context, streamMetadata);
            stream2.bind(context, streamMetadata);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.map(keyValueMapper);
            mapped2 = stream2.map(keyValueMapper);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = stream2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.map(keyValueMapper);
            mapped2 = stream2.map(keyValueMapper);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);
    }

    @Test
    public void testFlatMap() {
        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> mapped1;
        KStream<Integer, String> mapped2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;

        processor = new MockProcessor<>();

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.flatMap(keyValueMapper2);
    mapped2 = stream2.flatMap(keyValueMapper2);
>>>>>>> new api model
=======
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
    processor = new TestProcessor<>();
>>>>>>> wip
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
=======
        boolean exceptionRaised;
>>>>>>> compile and test passed

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMap(keyValueMapper2);
            mapped2 = stream2.flatMap(keyValueMapper2);

            exceptionRaised = false;
            windowed1 = stream1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMap(keyValueMapper2);
            mapped2 = stream2.flatMap(keyValueMapper2);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = stream2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMap(keyValueMapper2);
            mapped2 = stream2.flatMap(keyValueMapper2);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertTrue(exceptionRaised);
    }

    @Test
    public void testMapValues() {
        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> mapped1;
        KStream<Integer, String> mapped2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;

        KStreamTopology initializer = new MockKStreamTopology();
        processor = new MockProcessor<>();

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.mapValues(valueMapper);
    mapped2 = stream2.mapValues(valueMapper);
>>>>>>> new api model
=======
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
    processor = new TestProcessor<>();
>>>>>>> wip
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
=======
        boolean exceptionRaised;
>>>>>>> compile and test passed

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.mapValues(valueMapper);
            mapped2 = stream2.mapValues(valueMapper);

            exceptionRaised = false;
            windowed1 = stream1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.mapValues(valueMapper);
            mapped2 = stream2.mapValues(valueMapper);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = stream2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.mapValues(valueMapper);
            mapped2 = stream2.mapValues(valueMapper);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);
    }

    @Test
    public void testFlatMapValues() {
        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> mapped1;
        KStream<Integer, String> mapped2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessor<Integer, String> processor;

        KStreamTopology initializer = new MockKStreamTopology();
        processor = new MockProcessor<>();

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
    processor = new TestProcessor<>();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
    processor = new TestProcessor<>();
    stream1 = new KStreamSource<>(null, initializer);
    stream2 = new KStreamSource<>(null, initializer);
    mapped1 = stream1.flatMapValues(valueMapper2);
    mapped2 = stream2.flatMapValues(valueMapper2);
>>>>>>> new api model
=======
    KStreamTopology initializer = new MockKStreamTopology();
<<<<<<< HEAD
    processor = new TestProcessor<>();
>>>>>>> wip
=======
    processor = new MockProcessor<>();
>>>>>>> removing io.confluent imports: wip
=======
        boolean exceptionRaised;
>>>>>>> compile and test passed

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMapValues(valueMapper2);
            mapped2 = stream2.flatMapValues(valueMapper2);

            exceptionRaised = false;
            windowed1 = stream1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMapValues(valueMapper2);
            mapped2 = stream2.flatMapValues(valueMapper2);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = stream2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);

        try {
            stream1 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            stream2 = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
            mapped1 = stream1.flatMapValues(valueMapper2);
            mapped2 = stream2.flatMapValues(valueMapper2);

            exceptionRaised = false;
            windowed1 = mapped1.with(new UnlimitedWindow<>());
            windowed2 = mapped2.with(new UnlimitedWindow<>());

            windowed1.join(windowed2, joiner).process(processor);

            KStreamContext context = new MockProcessorContext(null, null);

        } catch (NotCopartitionedException e) {
            exceptionRaised = true;
        }

        assertFalse(exceptionRaised);
=======
>>>>>>> wip
    }

    // TODO: test for joinability
}
