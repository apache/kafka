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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class VersionedKeyValueStoreWithHeadersTopologyTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String STORE_NAME = "versioned-headers-store";
    private static final Duration HISTORY_RETENTION = Duration.ofMinutes(10);

    private StreamsBuilder builder() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION))
        ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    private Properties props() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-versioned-headers");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return props;
    }

    private static Headers headers(final String key, final String value) {
        return headers(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static Headers headers(final String key, final byte[] value) {
        return new RecordHeaders().add(key, value);
    }

    private static void assertHeaderValue(final Headers headers,
                                          final String key,
                                          final String expectedValue) {
        assertNotNull(headers.lastHeader(key));
        assertEquals(expectedValue, new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8));
    }

    private static void assertHeaderValue(final Headers headers,
                                          final String key,
                                          final byte[] expectedValue) {
        assertNotNull(headers.lastHeader(key));
        assertArrayEquals(expectedValue, headers.lastHeader(key).value());
    }

    @Test
    public void shouldMaterializeVersionedStoreWithHeaders() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder().build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            final Headers headers = headers("traceId", "trace-123");
            headers.add("schemaId", new byte[]{0, 0, 0, 42});

            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers, 1000L));

            final TestRecord<String, String> outputRecord = outputTopic.readRecord();
            assertNotNull(outputRecord);
            assertEquals("key1", outputRecord.key());
            assertEquals("value1", outputRecord.value());

            final VersionedKeyValueStore<String, String> store = driver.getVersionedKeyValueStore(STORE_NAME);
            assertInstanceOf(VersionedKeyValueStoreWithHeaders.class, store);
            final VersionedRecord<String> storedRecord = store.get("key1");
            assertEquals("value1", storedRecord.value());
            assertHeaderValue(storedRecord.headers(), "traceId", "trace-123");
            assertHeaderValue(storedRecord.headers(), "schemaId", new byte[]{0, 0, 0, 42});

            inputTopic.pipeInput(new TestRecord<>("key1", "value2", headers("traceId", "trace-456"), 2000L));

            final TestRecord<String, String> outputRecord2 = outputTopic.readRecord();
            assertNotNull(outputRecord2);
            assertEquals("key1", outputRecord2.key());
            assertEquals("value2", outputRecord2.value());

            final VersionedRecord<String> updatedRecord = store.get("key1");
            assertEquals("value2", updatedRecord.value());
            assertHeaderValue(updatedRecord.headers(), "traceId", "trace-456");
        }
    }

    @Test
    public void shouldCreateDualInterfaceSupplier() {
        final var supplier = Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION);

        assertInstanceOf(org.apache.kafka.streams.state.VersionedBytesStoreSupplier.class, supplier);
        assertInstanceOf(org.apache.kafka.streams.state.HeadersBytesStoreSupplier.class, supplier);
    }

    @Test
    public void shouldHandleMultipleVersionsWithHeaders() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder().build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput(new TestRecord<>("key1", "val-v1", headers("version", "v1"), 1000L));

            final TestRecord<String, String> out1 = outputTopic.readRecord();
            assertEquals("val-v1", out1.value());

            inputTopic.pipeInput(new TestRecord<>("key1", "val-v2", headers("version", "v2"), 2000L));

            final TestRecord<String, String> out2 = outputTopic.readRecord();
            assertEquals("val-v2", out2.value());

            inputTopic.pipeInput(new TestRecord<>("key1", "val-v3", headers("version", "v3"), 3000L));

            final TestRecord<String, String> out3 = outputTopic.readRecord();
            assertEquals("val-v3", out3.value());

            final VersionedKeyValueStore<String, String> store = driver.getVersionedKeyValueStore(STORE_NAME);
            final VersionedRecord<String> version1 = store.get("key1", 1000L);
            final VersionedRecord<String> version2 = store.get("key1", 2000L);
            final VersionedRecord<String> version3 = store.get("key1", 3000L);
            assertEquals("val-v1", version1.value());
            assertEquals("val-v2", version2.value());
            assertEquals("val-v3", version3.value());
            assertHeaderValue(version1.headers(), "version", "v1");
            assertHeaderValue(version2.headers(), "version", "v2");
            assertHeaderValue(version3.headers(), "version", "v3");
        }
    }

    @Test
    public void shouldHandleTombstonesWithHeaders() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder().build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers("op", "insert"), 1000L));

            final TestRecord<String, String> putOutput = outputTopic.readRecord();
            assertEquals("value1", putOutput.value());

            inputTopic.pipeInput(new TestRecord<>("key1", (String) null, new RecordHeaders(), 2000L));

            final TestRecord<String, String> deleteOutput = outputTopic.readRecord();
            assertNull(deleteOutput.value());
        }
    }

    @Test
    public void shouldForwardHeadersToChangelog() {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder().build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final String changelogTopic = "test-versioned-headers-" + STORE_NAME + "-changelog";

            final TestOutputTopic<String, String> changelogOutputTopic = driver.createOutputTopic(
                changelogTopic, new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers("traceId", "abc"), 1000L));

            final TestRecord<String, String> changelogRecord = changelogOutputTopic.readRecord();
            assertNotNull(changelogRecord);
            assertEquals("key1", changelogRecord.key());
            assertHeaderValue(changelogRecord.headers(), "traceId", "abc");
        }
    }
}
