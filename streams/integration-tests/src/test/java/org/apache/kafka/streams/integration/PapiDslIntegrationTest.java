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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class PapiDslIntegrationTest {

    @Test
    public void processorShouldAccessKTableStoreAsTimestampedStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table("input-topic", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStore<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueAndTimestamp<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueAndTimestamp<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build())) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKTableStoreAsHeadersStoreViaConfig() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table("input-topic", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = new Properties();
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders("table-store"));
        builder.table("input-topic", materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build())) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }
}
