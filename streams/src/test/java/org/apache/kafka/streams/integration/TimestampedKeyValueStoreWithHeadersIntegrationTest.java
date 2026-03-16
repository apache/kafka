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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Integration test to verify that key serializers can modify headers as a side-effect,
 * and that this side-effect makes it into the changelog topic.
 *
 * This test verifies the core assumption of the headers-aware state store implementation:
 * when we create a temporary context with new headers and serialize the key, the key
 * serializer will add metadata to those headers, and those headers
 * will be used when logging the change to the changelog topic.
 */
public class TimestampedKeyValueStoreWithHeadersIntegrationTest {

    private static final String STORE_NAME = "test-store";
    private static final String INPUT_TOPIC = "input";

    /**
     * Custom serializer that adds a header during serialization as a side-effect.
     * This simulates real-world serializers that add metadata to headers.
     */
    private static class HeaderAddingSerializer implements Serializer<String> {
        @Override
        public byte[] serialize(final String topic, final String data) {
            return data == null ? null : data.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final String data) {
            // Add metadata header during serialization (side-effect)
            headers.add("serializer-metadata", "test-value".getBytes(StandardCharsets.UTF_8));
            return serialize(topic, data);
        }
    }

    private static class HeaderAddingSerde implements Serde<String> {
        @Override
        public Serializer<String> serializer() {
            return new HeaderAddingSerializer();
        }

        @Override
        public Deserializer<String> deserializer() {
            return Serdes.String().deserializer();
        }
    }

    /**
     * Processor that puts and deletes from a timestamped key-value store with headers.
     */
    private static class StoreProcessor extends ContextualProcessor<String, String, Void, Void> {
        private TimestampedKeyValueStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            super.init(context);
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            if (record.value() == null) {
                // Delete
                store.delete(record.key());
            } else {
                // Put with timestamp and headers
                store.put(
                    record.key(),
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers())
                );
            }
        }
    }

    @Test
    public void shouldPropagateSerializerHeaderSideEffectToChangelog() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create a timestamped key-value store with headers using our custom serializer
        builder.addStateStore(
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                new HeaderAddingSerde(),  // Custom key serializer that adds headers
                Serdes.String()
            )
        );

        // Add a processor that uses the store
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .process(StoreProcessor::new, STORE_NAME);

        final Properties props = new Properties();
        props.put("application.id", "test-app");
        props.put("bootstrap.servers", "dummy:1234");
        props.put("state.dir", TestUtils.tempDirectory().getAbsolutePath());

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC,
                Serdes.String().serializer(),
                Serdes.String().serializer()
            );

            // Create output topic for changelog
            final String changelogTopic = "test-app-" + STORE_NAME + "-changelog";
            final TestOutputTopic<String, String> changelogOutputTopic =
                driver.createOutputTopic(
                    changelogTopic,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                );

            inputTopic.pipeInput("key1", "value1");

            // Verify changelog has the put record with header
            final var putRecord = changelogOutputTopic.readRecord();
            assertEquals("key1", putRecord.key());
            assertEquals("value1", putRecord.value());

            // Verify the serializer added metadata header as side-effect
            final Header putMetadataHeader = putRecord.headers().lastHeader("serializer-metadata");
            assertNotNull(putMetadataHeader, "metadata header should be present in put record");
            assertEquals("test-value", new String(putMetadataHeader.value(), StandardCharsets.UTF_8));

            inputTopic.pipeInput("key1", (String) null);

            // Verify changelog has the delete record (tombstone) with header
            final var deleteRecord = changelogOutputTopic.readRecord();
            assertEquals("key1", deleteRecord.key());
            assertNull(deleteRecord.value(), "Delete should produce tombstone (null value)");

            // CRITICAL: Verify the serializer's side-effect made it into the changelog
            // This is the core assumption we're testing!
            final Header deleteMetadataHeader = deleteRecord.headers().lastHeader("serializer-metadata");
            assertNotNull(deleteMetadataHeader,
                "metadata header should be present in tombstone - serializer side-effect must propagate to changelog");
            assertEquals("test-value", new String(deleteMetadataHeader.value(), StandardCharsets.UTF_8),
                "Tombstone should have metadata from serializer side-effect");
        }
    }
}
