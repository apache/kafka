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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ReadOnlyStoreTest {

    @Test
    public void shouldConnectProcessorAndWriteDataToReadOnlyStore() {
        final Topology topology = new Topology();
        topology.addReadOnlyStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("readOnlyStore"),
                new Serdes.IntegerSerde(),
                new Serdes.StringSerde()
            ),
            "readOnlySource",
            new IntegerDeserializer(),
            new StringDeserializer(),
            "storeTopic",
            "readOnlyProcessor",
            () -> new Processor<Integer, String, Void, Void>() {
                KeyValueStore<Integer, String> store;

                @Override
                public void init(final ProcessorContext<Void, Void> context) {
                    store = context.getStateStore("readOnlyStore");
                }
                @Override
                public void process(final Record<Integer, String> record) {
                    store.put(record.key(), record.value());
                }
            }
        );

        topology.addSource("source", new IntegerDeserializer(), new StringDeserializer(), "inputTopic");
        topology.addProcessor(
            "processor",
            () -> new Processor<Integer, String, Integer, String>() {
                ProcessorContext<Integer, String> context;
                KeyValueStore<Integer, String> store;

                @Override
                public void init(final ProcessorContext<Integer, String> context) {
                    this.context = context;
                    store = context.getStateStore("readOnlyStore");
                }

                @Override
                public void process(final Record<Integer, String> record) {
                    context.forward(record.withValue(
                        record.value() + " -- " + store.get(record.key())
                    ));
                }
            },
            "source"
        );
        topology.connectProcessorAndStateStores("processor", "readOnlyStore");
        topology.addSink("sink", "outputTopic", new IntegerSerializer(), new StringSerializer(), "processor");

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final TestInputTopic<Integer, String> readOnlyStoreTopic =
                driver.createInputTopic("storeTopic", new IntegerSerializer(), new StringSerializer());
            final TestInputTopic<Integer, String> input =
                driver.createInputTopic("inputTopic", new IntegerSerializer(), new StringSerializer());
            final TestOutputTopic<Integer, String> output =
                driver.createOutputTopic("outputTopic", new IntegerDeserializer(), new StringDeserializer());

            readOnlyStoreTopic.pipeInput(1, "foo");
            readOnlyStoreTopic.pipeInput(2, "bar");

            input.pipeInput(1, "bar");
            input.pipeInput(2, "foo");

            final KeyValueStore<Integer, String> store = driver.getKeyValueStore("readOnlyStore");

            try (final KeyValueIterator<Integer, String> it = store.all()) {
                final List<KeyValue<Integer, String>> storeContent = new LinkedList<>();
                it.forEachRemaining(storeContent::add);

                final List<KeyValue<Integer, String>> expectedResult = new LinkedList<>();
                expectedResult.add(KeyValue.pair(1, "foo"));
                expectedResult.add(KeyValue.pair(2, "bar"));

                assertThat(storeContent, equalTo(expectedResult));
            }

            final List<KeyValue<Integer, String>> expectedResult = new LinkedList<>();
            expectedResult.add(KeyValue.pair(1, "bar -- foo"));
            expectedResult.add(KeyValue.pair(2, "foo -- bar"));

            assertThat(output.readKeyValuesToList(), equalTo(expectedResult));
        }
    }
}
