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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.asList;


public class KStreamNewProcessorApiTest {

    @Test
    void shouldGetStateStoreWithConnectedStoreProvider() {
        runTest(false);
    }

    @Test
    void shouldGetStateStoreWithStreamBuilder() {
        runTest(true);
    }

    private void runTest(final boolean shouldAddStoreDirectly) {
        final StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<?> storeBuilder = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("store"), Serdes.String(), Serdes.String());

        if (shouldAddStoreDirectly) {
            builder.addStateStore(storeBuilder);
        }
        builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(new TransformerSupplier(shouldAddStoreDirectly ? null : storeBuilder), "store")
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        final List<KeyValue<String, String>> words = Arrays.asList(KeyValue.pair("a", "foo"), KeyValue.pair("b", "bar"), KeyValue.pair("c", "baz"));
        try (TopologyTestDriver testDriver = new TopologyTestDriver(builder.build())) {
            final TestInputTopic<String, String>
                    testDriverInputTopic =
                    testDriver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());

            words.forEach(clk -> testDriverInputTopic.pipeInput(clk.key, clk.value));

            final List<String> expectedOutput = asList("fooUpdated", "barUpdated", "bazUpdated");

            final Deserializer<String> keyDeserializer = Serdes.String().deserializer();
            final List<String> actualOutput =
                    new ArrayList<>(testDriver.createOutputTopic("output", keyDeserializer, Serdes.String().deserializer()).readValuesToList());

            final KeyValueStore<String, String> stateStore = testDriver.getKeyValueStore("store");

            Assertions.assertEquals(expectedOutput, actualOutput);
            Assertions.assertEquals(stateStore.get("a"), "fooUpdated");
            Assertions.assertEquals(stateStore.get("b"), "barUpdated");
            Assertions.assertEquals(stateStore.get("c"), "bazUpdated");
        }
    }
    
    private static class TransformerSupplier implements FixedKeyProcessorSupplier<String, String, String> {
        private final StoreBuilder<?> storeBuilder;
        public TransformerSupplier(final StoreBuilder<?> storeBuilder) {
            this.storeBuilder = storeBuilder;
        }


        @Override
        public ContextualFixedKeyProcessor<String, String, String> get() {
            return new ContextualFixedKeyProcessor<String, String, String>() {
                KeyValueStore<String, String> store;
                FixedKeyProcessorContext<String, String> context;

                @Override
                public void init(final FixedKeyProcessorContext<String, String> context) {
                    super.init(context);
                    store = context.getStateStore("store");
                    Objects.requireNonNull(store, "State store can't be null");
                    this.context = context;
                }

                @Override
                public void process(final FixedKeyRecord<String, String> record) {
                    store.putIfAbsent(record.key(), record.value() + "Updated");
                    context().forward(record.withValue(record.value() + "Updated"));
                }

            };
        }
        @Override
        public Set<StoreBuilder<?>> stores() {
            if (storeBuilder != null) {
                return Collections.singleton(storeBuilder);
            }
            return null;
        }
    }
}
