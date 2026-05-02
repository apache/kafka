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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link TimestampedWindowStoreWithHeaders}.
 * <p>
 * Exercises the public interface through the {@link Stores#timestampedWindowStoreWithHeadersBuilder}
 * factory paired with an in-memory window bytes supplier. The goal is to verify that values,
 * timestamps, and headers round-trip through the public API methods — including
 * {@code backwardFetch} and {@code fetchAll} — which were flagged in the KAFKA-20328
 * description as the class of method most likely to have missing overrides on wrappers.
 */
public class TimestampedWindowStoreWithHeadersContractTest {

    private static final long WINDOW_SIZE = 10L;
    private static final long RETENTION = 1000L;

    private TimestampedWindowStoreWithHeaders<String, String> store;
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
        store = Stores.timestampedWindowStoreWithHeadersBuilder(
            Stores.inMemoryWindowStore(
                "contract-window-store",
                Duration.ofMillis(RETENTION),
                Duration.ofMillis(WINDOW_SIZE),
                false
            ),
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
    public void shouldRoundTripValueTimestampAndHeadersViaPutAndFetch() {
        final Headers headers = headersWith("schema-id", "42");
        store.put("k", ValueTimestampHeaders.make("v", 100L, headers), 100L);

        try (WindowStoreIterator<ValueTimestampHeaders<String>> it = store.fetch("k", 100L, 100L)) {
            assertTrue(it.hasNext());
            final KeyValue<Long, ValueTimestampHeaders<String>> next = it.next();
            assertEquals(100L, next.key.longValue());
            assertEquals("v", next.value.value());
            assertEquals(100L, next.value.timestamp());
            assertEquals(headers, next.value.headers());
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void shouldReturnEmptyIteratorForMissingKey() {
        try (WindowStoreIterator<ValueTimestampHeaders<String>> it = store.fetch("missing", 0L, 1000L)) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void shouldPreserveHeadersAcrossMultipleWindows() {
        final Headers h1 = headersWith("id", "w1");
        final Headers h2 = headersWith("id", "w2");
        final Headers h3 = headersWith("id", "w3");

        store.put("k", ValueTimestampHeaders.make("v1", 100L, h1), 100L);
        store.put("k", ValueTimestampHeaders.make("v2", 200L, h2), 200L);
        store.put("k", ValueTimestampHeaders.make("v3", 300L, h3), 300L);

        final List<KeyValue<Long, Headers>> collected = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<String>> it = store.fetch("k", 100L, 300L)) {
            while (it.hasNext()) {
                final KeyValue<Long, ValueTimestampHeaders<String>> next = it.next();
                collected.add(KeyValue.pair(next.key, next.value.headers()));
            }
        }

        assertEquals(3, collected.size());
        assertEquals(100L, collected.get(0).key.longValue());
        assertEquals(h1, collected.get(0).value);
        assertEquals(200L, collected.get(1).key.longValue());
        assertEquals(h2, collected.get(1).value);
        assertEquals(300L, collected.get(2).key.longValue());
        assertEquals(h3, collected.get(2).value);
    }

    @Test
    public void shouldPreserveHeadersAcrossBackwardFetch() {
        final Headers h1 = headersWith("id", "early");
        final Headers h2 = headersWith("id", "late");

        store.put("k", ValueTimestampHeaders.make("v1", 100L, h1), 100L);
        store.put("k", ValueTimestampHeaders.make("v2", 200L, h2), 200L);

        final List<KeyValue<Long, Headers>> collected = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<String>> it = store.backwardFetch("k", 100L, 200L)) {
            while (it.hasNext()) {
                final KeyValue<Long, ValueTimestampHeaders<String>> next = it.next();
                collected.add(KeyValue.pair(next.key, next.value.headers()));
            }
        }

        assertEquals(2, collected.size());
        assertEquals(200L, collected.get(0).key.longValue());
        assertEquals(h2, collected.get(0).value);
        assertEquals(100L, collected.get(1).key.longValue());
        assertEquals(h1, collected.get(1).value);
    }

    @Test
    public void shouldPreserveHeadersAcrossFetchAll() {
        final Headers h1 = headersWith("id", "a");
        final Headers h2 = headersWith("id", "b");

        store.put("a", ValueTimestampHeaders.make("va", 100L, h1), 100L);
        store.put("b", ValueTimestampHeaders.make("vb", 100L, h2), 100L);

        int count = 0;
        try (KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> it =
                 store.fetchAll(100L, 100L)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> next = it.next();
                if ("a".equals(next.key.key())) {
                    assertEquals(h1, next.value.headers());
                } else if ("b".equals(next.key.key())) {
                    assertEquals(h2, next.value.headers());
                }
                count++;
            }
        }
        assertEquals(2, count);
    }

    @Test
    public void shouldPreserveHeadersAcrossBackwardFetchAll() {
        final Headers h1 = headersWith("id", "early");
        final Headers h2 = headersWith("id", "late");

        store.put("k", ValueTimestampHeaders.make("v1", 100L, h1), 100L);
        store.put("k", ValueTimestampHeaders.make("v2", 200L, h2), 200L);

        final List<Long> timestamps = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> it =
                 store.backwardFetchAll(100L, 200L)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> next = it.next();
                timestamps.add(next.key.window().start());
                if (next.key.window().start() == 100L) {
                    assertEquals(h1, next.value.headers());
                } else {
                    assertEquals(h2, next.value.headers());
                }
            }
        }
        assertEquals(2, timestamps.size());
        assertTrue(timestamps.get(0) >= timestamps.get(1),
            "backwardFetchAll should return newer windows first");
    }

    @Test
    public void shouldPreserveEmptyHeaders() {
        store.put("k", ValueTimestampHeaders.make("v", 100L, new RecordHeaders()), 100L);

        try (WindowStoreIterator<ValueTimestampHeaders<String>> it = store.fetch("k", 100L, 100L)) {
            assertTrue(it.hasNext());
            final ValueTimestampHeaders<String> result = it.next().value;
            assertEquals("v", result.value());
            assertEquals(new RecordHeaders(), result.headers());
        }
    }

    private static Headers headersWith(final String key, final String value) {
        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }
}
