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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link TimestampedKeyValueStoreWithHeaders}.
 * <p>
 * Exercises the public interface through the {@link Stores#timestampedKeyValueStoreWithHeadersBuilder}
 * factory paired with an in-memory bytes supplier. The goal is to verify that values, timestamps,
 * and headers round-trip through the public API methods — including range iteration and
 * {@code reverseRange} — so that regressions in the wrapped headers byte layout are caught
 * at the interface level rather than only through end-to-end integration tests.
 */
public class TimestampedKeyValueStoreWithHeadersContractTest {

    private TimestampedKeyValueStoreWithHeaders<String, String> store;
    private InternalMockProcessorContext<String, String> context;

    @BeforeEach
    public void setUp() {
        final File dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        context = new InternalMockProcessorContext<>(
            dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props)
        );
        store = Stores.timestampedKeyValueStoreWithHeadersBuilder(
            Stores.inMemoryKeyValueStore("contract-store"),
            Serdes.String(),
            Serdes.String()
        ).withLoggingDisabled().withCachingDisabled().build();
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void shouldRoundTripValueTimestampAndHeadersViaPutAndGet() {
        final Headers headers = headersWith("schema-id", "42");
        store.put("k1", ValueTimestampHeaders.make("v1", 1000L, headers));

        final ValueTimestampHeaders<String> result = store.get("k1");
        assertEquals("v1", result.value());
        assertEquals(1000L, result.timestamp());
        assertEquals(headers, result.headers());
    }

    @Test
    public void shouldReturnNullForMissingKey() {
        assertNull(store.get("missing"));
    }

    @Test
    public void shouldTreatNullValueAsTombstone() {
        final Headers headers = headersWith("h", "v");
        store.put("k", ValueTimestampHeaders.make("value", 10L, headers));
        assertEquals("value", store.get("k").value());

        store.put("k", null);
        assertNull(store.get("k"));
    }

    @Test
    public void shouldDeleteKeyAndReturnPriorValue() {
        final Headers headers = headersWith("h", "v");
        store.put("k", ValueTimestampHeaders.make("value", 10L, headers));

        final ValueTimestampHeaders<String> deleted = store.delete("k");
        assertEquals("value", deleted.value());
        assertEquals(10L, deleted.timestamp());
        assertEquals(headers, deleted.headers());
        assertNull(store.get("k"));
    }

    @Test
    public void shouldPreserveHeadersAcrossRange() {
        final Headers h1 = headersWith("id", "1");
        final Headers h2 = headersWith("id", "2");
        final Headers h3 = headersWith("id", "3");

        store.put("a", ValueTimestampHeaders.make("va", 100L, h1));
        store.put("b", ValueTimestampHeaders.make("vb", 200L, h2));
        store.put("c", ValueTimestampHeaders.make("vc", 300L, h3));

        final List<KeyValue<String, ValueTimestampHeaders<String>>> collected = new ArrayList<>();
        try (KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.range("a", "c")) {
            while (it.hasNext()) {
                collected.add(it.next());
            }
        }

        assertEquals(3, collected.size());
        assertEquals("a", collected.get(0).key);
        assertEquals(h1, collected.get(0).value.headers());
        assertEquals("b", collected.get(1).key);
        assertEquals(h2, collected.get(1).value.headers());
        assertEquals("c", collected.get(2).key);
        assertEquals(h3, collected.get(2).value.headers());
    }

    @Test
    public void shouldPreserveHeadersAcrossReverseRange() {
        final Headers h1 = headersWith("id", "1");
        final Headers h2 = headersWith("id", "2");

        store.put("a", ValueTimestampHeaders.make("va", 100L, h1));
        store.put("b", ValueTimestampHeaders.make("vb", 200L, h2));

        final List<KeyValue<String, ValueTimestampHeaders<String>>> collected = new ArrayList<>();
        try (KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.reverseRange("a", "b")) {
            while (it.hasNext()) {
                collected.add(it.next());
            }
        }

        assertEquals(2, collected.size());
        assertEquals("b", collected.get(0).key);
        assertEquals(h2, collected.get(0).value.headers());
        assertEquals("a", collected.get(1).key);
        assertEquals(h1, collected.get(1).value.headers());
    }

    @Test
    public void shouldReturnEmptyIteratorWhenStoreIsEmpty() {
        try (KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void shouldPutIfAbsentAndNotOverwriteExisting() {
        final Headers first = headersWith("h", "first");
        final Headers second = headersWith("h", "second");

        assertNull(store.putIfAbsent("k", ValueTimestampHeaders.make("v1", 1L, first)));

        final ValueTimestampHeaders<String> previous =
            store.putIfAbsent("k", ValueTimestampHeaders.make("v2", 2L, second));
        assertEquals("v1", previous.value());
        assertEquals(first, previous.headers());
        assertEquals("v1", store.get("k").value());
        assertEquals(first, store.get("k").headers());
    }

    @Test
    public void shouldPreserveEmptyHeaders() {
        store.put("k", ValueTimestampHeaders.make("v", 10L, new RecordHeaders()));

        final ValueTimestampHeaders<String> result = store.get("k");
        assertEquals("v", result.value());
        assertEquals(new RecordHeaders(), result.headers());
        assertTrue(result.headers().toArray().length == 0);
    }

    private static Headers headersWith(final String key, final String value) {
        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }
}
