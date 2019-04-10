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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private final ForeachAction<Integer, Integer> action = (key, value) -> results.add(KeyValue.pair(key, value));
    private KStream<Integer, Integer> stream;

    @Before
    public void before() {
        builder = new StreamsBuilder();
        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                            Serdes.Integer(),
                                            Serdes.Integer());
        builder.addStateStore(keyValueStoreBuilder);
        stream = builder.stream(topic, Consumed.with(Serdes.Integer(), Serdes.Integer()));
    }

    private void verifyResult(final List<KeyValue<Integer, Integer>> expected) {
        final ConsumerRecordFactory<Integer, Integer> recordFactory =
            new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic, Arrays.asList(new KeyValue<>(1, 1),
                                                                       new KeyValue<>(2, 2),
                                                                       new KeyValue<>(3, 3),
                                                                       new KeyValue<>(2, 1),
                                                                       new KeyValue<>(2, 3),
                                                                       new KeyValue<>(1, 3))));
        }
        assertThat(results, equalTo(expected));
    }

    @Test
    public void shouldTransform() {
        stream
            .transform(() -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
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
            }, "myTransformState")
            .foreach(action);

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
    public void shouldFlatTransform() {
        stream
            .flatTransform(() -> new Transformer<Integer, Integer, Iterable<KeyValue<Integer, Integer>>>() {
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
            }, "myTransformState")
            .foreach(action);

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
    public void shouldTransformValuesWithValueTransformerWithKey() {
        stream
            .transformValues(() -> new ValueTransformerWithKey<Integer, Integer, Integer>() {
                private KeyValueStore<Integer, Integer> state;

                @Override
                public void init(final ProcessorContext context) {
                    state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
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
            }, "myTransformState")
            .foreach(action);

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
    public void shouldTransformValuesWithValueTransformerWithoutKey() {
        stream
            .transformValues(() -> new ValueTransformer<Integer, Integer>() {
                private KeyValueStore<Integer, Integer> state;

                @Override
                public void init(final ProcessorContext context) {
                    state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
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
            }, "myTransformState")
            .foreach(action);

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
    public void shouldFlatTransformValuesWithKey() {
        stream
            .flatTransformValues(() -> new ValueTransformerWithKey<Integer, Integer, Iterable<Integer>>() {
                private KeyValueStore<Integer, Integer> state;

                @Override
                public void init(final ProcessorContext context) {
                    state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
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
            }, "myTransformState")
            .foreach(action);

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
    public void shouldFlatTransformValuesWithValueTransformerWithoutKey() {
        stream
            .flatTransformValues(() -> new ValueTransformer<Integer, Iterable<Integer>>() {
                private KeyValueStore<Integer, Integer> state;

                @Override
                public void init(final ProcessorContext context) {
                    state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
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
            }, "myTransformState")
            .foreach(action);

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
