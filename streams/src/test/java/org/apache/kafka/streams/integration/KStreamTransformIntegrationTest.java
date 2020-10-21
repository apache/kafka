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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class KStreamTransformIntegrationTest {

    private StreamsBuilder builder;
    private final String topic = "stream";
    private final String stateStoreName = "myTransformState";
    private final List<KeyValue<Integer, Integer>> results = new ArrayList<>();
    private final ForeachAction<Integer, Integer> accumulateExpected = (key, value) -> results.add(KeyValue.pair(key, value));
    private KStream<Integer, Integer> stream;

    @Before
    public void before() {
        builder = new StreamsBuilder();
        stream = builder.stream(topic, Consumed.with(Serdes.Integer(), Serdes.Integer()));
    }

    private StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder() {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                    Serdes.Integer(),
                                    Serdes.Integer());
    }

    private void verifyResult(final List<KeyValue<Integer, Integer>> expected) {
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, Integer> inputTopic =
                driver.createInputTopic(topic, new IntegerSerializer(), new IntegerSerializer());
            inputTopic.pipeKeyValueList(Arrays.asList(
                new KeyValue<>(1, 1),
                new KeyValue<>(2, 2),
                new KeyValue<>(3, 3),
                new KeyValue<>(2, 1),
                new KeyValue<>(2, 3),
                new KeyValue<>(1, 3)));
        }
        assertThat(results, equalTo(expected));
    }

    private class TestTransformer implements Transformer<Integer, Integer, KeyValue<Integer, Integer>> {
        private KeyValueStore<Integer, Integer> state;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
            state.putIfAbsent(key, 0);
            Integer storedValue = state.get(key);
            final KeyValue<Integer, Integer> result = new KeyValue<>(key + 1, value + storedValue++);
            state.put(key, storedValue);
            return result;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldTransform() {
        builder.addStateStore(storeBuilder());

        stream
            .transform(TestTransformer::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(2, 1),
            KeyValue.pair(3, 2),
            KeyValue.pair(4, 3),
            KeyValue.pair(3, 2),
            KeyValue.pair(3, 5),
            KeyValue.pair(2, 4));
        verifyResult(expected);
    }

    @Test
    public void shouldTransformWithConnectedStoreProvider() {
        stream
            .transform(new TransformerSupplier<Integer, Integer, KeyValue<Integer, Integer>>() {
                @Override
                public Transformer<Integer, Integer, KeyValue<Integer, Integer>> get() {
                    return new TestTransformer();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(2, 1),
            KeyValue.pair(3, 2),
            KeyValue.pair(4, 3),
            KeyValue.pair(3, 2),
            KeyValue.pair(3, 5),
            KeyValue.pair(2, 4));
        verifyResult(expected);
    }

    private class TestFlatTransformer implements Transformer<Integer, Integer, Iterable<KeyValue<Integer, Integer>>> {
        private KeyValueStore<Integer, Integer> state;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public Iterable<KeyValue<Integer, Integer>> transform(final Integer key, final Integer value) {
            final List<KeyValue<Integer, Integer>> result = new ArrayList<>();
            state.putIfAbsent(key, 0);
            Integer storedValue = state.get(key);
            for (int i = 0; i < 3; i++) {
                result.add(new KeyValue<>(key + i, value + storedValue++));
            }
            state.put(key, storedValue);
            return result;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldFlatTransform() {
        builder.addStateStore(storeBuilder());

        stream
            .flatTransform(TestFlatTransformer::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(2, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(4, 4),
            KeyValue.pair(3, 3),
            KeyValue.pair(4, 4),
            KeyValue.pair(5, 5),
            KeyValue.pair(2, 4),
            KeyValue.pair(3, 5),
            KeyValue.pair(4, 6),
            KeyValue.pair(2, 9),
            KeyValue.pair(3, 10),
            KeyValue.pair(4, 11),
            KeyValue.pair(1, 6),
            KeyValue.pair(2, 7),
            KeyValue.pair(3, 8));
        verifyResult(expected);
    }

    @Test
    public void shouldFlatTransformWithConnectedStoreProvider() {
        stream
            .flatTransform(new TransformerSupplier<Integer, Integer, Iterable<KeyValue<Integer, Integer>>>() {
                @Override
                public Transformer<Integer, Integer, Iterable<KeyValue<Integer, Integer>>> get() {
                    return new TestFlatTransformer();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(2, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(4, 4),
            KeyValue.pair(3, 3),
            KeyValue.pair(4, 4),
            KeyValue.pair(5, 5),
            KeyValue.pair(2, 4),
            KeyValue.pair(3, 5),
            KeyValue.pair(4, 6),
            KeyValue.pair(2, 9),
            KeyValue.pair(3, 10),
            KeyValue.pair(4, 11),
            KeyValue.pair(1, 6),
            KeyValue.pair(2, 7),
            KeyValue.pair(3, 8));
        verifyResult(expected);
    }

    private class TestValueTransformerWithKey implements ValueTransformerWithKey<Integer, Integer, Integer> {
        private KeyValueStore<Integer, Integer> state;

        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public Integer transform(final Integer key, final Integer value) {
            state.putIfAbsent(key, 0);
            Integer storedValue = state.get(key);
            final Integer result = value + storedValue++;
            state.put(key, storedValue);
            return result;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldTransformValuesWithValueTransformerWithKey() {
        builder.addStateStore(storeBuilder());

        stream
            .transformValues(TestValueTransformerWithKey::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 5),
            KeyValue.pair(1, 4));
        verifyResult(expected);
    }

    @Test
    public void shouldTransformValuesWithValueTransformerWithKeyWithConnectedStoreProvider() {
        stream
            .transformValues(new ValueTransformerWithKeySupplier<Integer, Integer, Integer>() {
                @Override
                public ValueTransformerWithKey<Integer, Integer, Integer> get() {
                    return new TestValueTransformerWithKey();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);
    }

    private class TestValueTransformer implements ValueTransformer<Integer, Integer> {
        private KeyValueStore<Integer, Integer> state;

        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public Integer transform(final Integer value) {
            state.putIfAbsent(value, 0);
            Integer counter = state.get(value);
            state.put(value, ++counter);
            return counter;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldTransformValuesWithValueTransformerWithoutKey() {
        builder.addStateStore(storeBuilder());

        stream
            .transformValues(TestValueTransformer::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(2, 1),
            KeyValue.pair(3, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 2),
            KeyValue.pair(1, 3));
        verifyResult(expected);
    }

    @Test
    public void shouldTransformValuesWithValueTransformerWithoutKeyWithConnectedStoreProvider() {
        stream
            .transformValues(new ValueTransformerSupplier<Integer, Integer>() {
                @Override
                public ValueTransformer<Integer, Integer> get() {
                    return new TestValueTransformer();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(2, 1),
            KeyValue.pair(3, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 2),
            KeyValue.pair(1, 3));
        verifyResult(expected);
    }

    private class TestValueTransformerWithoutKey implements ValueTransformerWithKey<Integer, Integer, Iterable<Integer>> {
        private KeyValueStore<Integer, Integer> state;

        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public Iterable<Integer> transform(final Integer key, final Integer value) {
            final List<Integer> result = new ArrayList<>();
            state.putIfAbsent(key, 0);
            Integer storedValue = state.get(key);
            for (int i = 0; i < 3; i++) {
                result.add(value + storedValue++);
            }
            state.put(key, storedValue);
            return result;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldFlatTransformValuesWithKey() {
        builder.addStateStore(storeBuilder());

        stream
            .flatTransformValues(TestValueTransformerWithoutKey::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(1, 2),
            KeyValue.pair(1, 3),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 3),
            KeyValue.pair(2, 4),
            KeyValue.pair(3, 3),
            KeyValue.pair(3, 4),
            KeyValue.pair(3, 5),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(2, 9),
            KeyValue.pair(2, 10),
            KeyValue.pair(2, 11),
            KeyValue.pair(1, 6),
            KeyValue.pair(1, 7),
            KeyValue.pair(1, 8));
        verifyResult(expected);
    }

    @Test
    public void shouldFlatTransformValuesWithKeyWithConnectedStoreProvider() {
        stream
            .flatTransformValues(new ValueTransformerWithKeySupplier<Integer, Integer, Iterable<Integer>>() {
                @Override
                public ValueTransformerWithKey<Integer, Integer, Iterable<Integer>> get() {
                    return new TestValueTransformerWithoutKey();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(1, 2),
            KeyValue.pair(1, 3),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 3),
            KeyValue.pair(2, 4),
            KeyValue.pair(3, 3),
            KeyValue.pair(3, 4),
            KeyValue.pair(3, 5),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(2, 9),
            KeyValue.pair(2, 10),
            KeyValue.pair(2, 11),
            KeyValue.pair(1, 6),
            KeyValue.pair(1, 7),
            KeyValue.pair(1, 8));
        verifyResult(expected);
    }

    private class TestFlatValueTransformer implements ValueTransformer<Integer, Iterable<Integer>> {
        private KeyValueStore<Integer, Integer> state;

        @Override
        public void init(final ProcessorContext context) {
            state = (KeyValueStore<Integer, Integer>) context.getStateStore(stateStoreName);
        }

        @Override
        public Iterable<Integer> transform(final Integer value) {
            final List<Integer> result = new ArrayList<>();
            state.putIfAbsent(value, 0);
            Integer counter = state.get(value);
            for (int i = 0; i < 3; i++) {
                result.add(++counter);
            }
            state.put(value, counter);
            return result;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void shouldFlatTransformValuesWithValueTransformerWithoutKey() {
        builder.addStateStore(storeBuilder());

        stream
            .flatTransformValues(TestFlatValueTransformer::new, stateStoreName)
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(1, 2),
            KeyValue.pair(1, 3),
            KeyValue.pair(2, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 3),
            KeyValue.pair(3, 1),
            KeyValue.pair(3, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(1, 7),
            KeyValue.pair(1, 8),
            KeyValue.pair(1, 9));
        verifyResult(expected);
    }

    @Test
    public void shouldFlatTransformValuesWithValueTransformerWithoutKeyWithConnectedStoreProvider() {
        stream
            .flatTransformValues(new ValueTransformerSupplier<Integer, Iterable<Integer>>() {
                @Override
                public ValueTransformer<Integer, Iterable<Integer>> get() {
                    return new TestFlatValueTransformer();
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    return Collections.singleton(storeBuilder());
                }
            })
            .foreach(accumulateExpected);

        final List<KeyValue<Integer, Integer>> expected = Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(1, 2),
            KeyValue.pair(1, 3),
            KeyValue.pair(2, 1),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 3),
            KeyValue.pair(3, 1),
            KeyValue.pair(3, 2),
            KeyValue.pair(3, 3),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(2, 4),
            KeyValue.pair(2, 5),
            KeyValue.pair(2, 6),
            KeyValue.pair(1, 7),
            KeyValue.pair(1, 8),
            KeyValue.pair(1, 9));
        verifyResult(expected);
    }
}