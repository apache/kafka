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
package org.apache.kafka.streams.state.internals;

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
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test to verify that key serializers can modify headers as a side-effect,
 * and that this side-effect makes it into the changelog topic for window stores.
 *
 * This test verifies the core assumption of the headers-aware state store implementation:
 * when we create a temporary context with new headers and serialize the key, the key
 * serializer will add metadata to those headers, and those headers
 * will be used when logging the change to the changelog topic.
 */
public class TimestampedWindowStoreWithHeadersSerializerSideEffectTest {

    private static final String STORE_NAME = "test-window-store";
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final long WINDOW_SIZE_MS = 10000L;

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
            headers.add("serializer-metadata", "window-test-value".getBytes(StandardCharsets.UTF_8));
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
     * Processor that puts and deletes from a timestamped window store with headers.
     * Uses command value "put(null)" to test deletion via put(key, null, windowStartTimestamp).
     */
    private static class WindowStoreProcessor extends ContextualProcessor<String, String, String, String> {
        private TimestampedWindowStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<String, String> context) {
            super.init(context);
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            final long windowStartTimestamp = record.timestamp();
            if ("put(null)".equals(record.value())) {
                // Delete using put(key, null, windowStartTimestamp)
                store.put(record.key(), null, windowStartTimestamp);
            } else {
                // Put with timestamp and headers
                store.put(
                    record.key(),
                    ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()),
                    windowStartTimestamp
                );
            }

            context().forward(record);
        }
    }

    @Test
    public void shouldPropagateSerializerHeaderSideEffectToChangelog() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create a timestamped window store with headers using our custom serializer
        builder.addStateStore(
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.persistentTimestampedWindowStore(
                    STORE_NAME,
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    Duration.ofMillis(WINDOW_SIZE_MS),
                    false
                ),
                new HeaderAddingSerde(),  // Custom key serializer that adds headers
                Serdes.String()
            )
        );

        // Add a processor that uses the store and forwards to output
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .process(WindowStoreProcessor::new, STORE_NAME)
            .to(OUTPUT_TOPIC);

        final Properties props = new Properties();
        props.put("application.id", "test-window-app");
        props.put("bootstrap.servers", "dummy:1234");
        props.put("state.dir", TestUtils.tempDirectory().getAbsolutePath());
        props.put("default.key.serde", Serdes.StringSerde.class);
        props.put("default.value.serde", Serdes.StringSerde.class);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC,
                Serdes.String().serializer(),
                Serdes.String().serializer()
            );

            final String changelogTopic = "test-window-app-" + STORE_NAME + "-changelog";
            final TestOutputTopic<String, String> changelogOutputTopic =
                driver.createOutputTopic(
                    changelogTopic,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                );

            final TestOutputTopic<String, String> outputTopic =
                driver.createOutputTopic(
                    OUTPUT_TOPIC,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                );

            inputTopic.pipeInput("key1", "value1", 1000L);

            // Verify changelog has the put record with header
            final var putChangelogRecord = changelogOutputTopic.readRecord();
            assertNotNull(putChangelogRecord.key());
            assertEquals("value1", putChangelogRecord.value());

            // Verify the serializer added metadata header to changelog
            final Header putMetadataHeader = putChangelogRecord.headers().lastHeader("serializer-metadata");
            assertNotNull(putMetadataHeader, "metadata header should be present in changelog put record");
            assertEquals("window-test-value", new String(putMetadataHeader.value(), StandardCharsets.UTF_8));

            final var putOutputRecord = outputTopic.readRecord();
            assertEquals("key1", putOutputRecord.key());
            assertEquals("value1", putOutputRecord.value());
            final Header outputMetadataHeader = putOutputRecord.headers().lastHeader("serializer-metadata");
            assertNotNull(outputMetadataHeader,
                "Output record SHOULD contain serializer-metadata header for normal put operations");
            assertEquals("window-test-value", new String(outputMetadataHeader.value(), StandardCharsets.UTF_8));

            inputTopic.pipeInput("key1", "put(null)", 1000L);

            // Verify changelog has the delete record (tombstone) with header
            final var putNullChangelogRecord = changelogOutputTopic.readRecord();
            assertNotNull(putNullChangelogRecord.key());
            assertNull(putNullChangelogRecord.value(), "put(null) should produce tombstone");

            // Verify the serializer's side-effect made it into the changelog
            final Header putNullMetadataHeader = putNullChangelogRecord.headers().lastHeader("serializer-metadata");
            assertNotNull(putNullMetadataHeader, "metadata header should be present in changelog tombstone from put(null)");
            assertEquals("window-test-value", new String(putNullMetadataHeader.value(), StandardCharsets.UTF_8));

            // CRITICAL: Verify output record does NOT contain the serializer-added header
            final var putNullOutputRecord = outputTopic.readRecord();
            assertEquals("key1", putNullOutputRecord.key());
            assertEquals("put(null)", putNullOutputRecord.value());
            final Header outputPutNullMetadataHeader = putNullOutputRecord.headers().lastHeader("serializer-metadata");
            assertNull(outputPutNullMetadataHeader, "Output record should NOT contain serializer-metadata header - side-effect should be isolated to changelog");
        }
    }
}
