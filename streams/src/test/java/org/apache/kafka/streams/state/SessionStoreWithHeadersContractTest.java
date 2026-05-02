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
import org.apache.kafka.streams.kstream.internals.SessionWindow;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link SessionStoreWithHeaders}.
 */
public class SessionStoreWithHeadersContractTest {

    private static final long RETENTION = 10_000L;

    private SessionStoreWithHeaders<String, String> store;
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
        store = Stores.sessionStoreWithHeadersBuilder(
            Stores.inMemorySessionStore("contract-session-store", Duration.ofMillis(RETENTION)),
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
    public void shouldRoundTripAggregationAndHeadersViaPutAndFetchSession() {
        final Headers headers = headersWith("schema-id", "42");
        final Windowed<String> key = windowed("k", 100L, 200L);

        store.put(key, AggregationWithHeaders.make("agg", headers));

        final AggregationWithHeaders<String> result = store.fetchSession("k", 100L, 200L);
        assertEquals("agg", result.aggregation());
        assertEquals(headers, result.headers());
    }

    @Test
    public void shouldReturnNullForMissingSession() {
        assertNull(store.fetchSession("missing", 0L, 10L));
    }

    @Test
    public void shouldTreatNullAggregationAsTombstone() {
        final Headers headers = headersWith("h", "v");
        final Windowed<String> key = windowed("k", 100L, 200L);

        store.put(key, AggregationWithHeaders.make("agg", headers));
        assertEquals("agg", store.fetchSession("k", 100L, 200L).aggregation());

        store.put(key, null);
        assertNull(store.fetchSession("k", 100L, 200L));
    }

    @Test
    public void shouldRemoveSessionByWindowedKey() {
        final Headers headers = headersWith("h", "v");
        final Windowed<String> key = windowed("k", 100L, 200L);

        store.put(key, AggregationWithHeaders.make("agg", headers));
        store.remove(key);

        assertNull(store.fetchSession("k", 100L, 200L));
    }

    @Test
    public void shouldPreserveHeadersAcrossFetchByKey() {
        final Headers h1 = headersWith("id", "1");
        final Headers h2 = headersWith("id", "2");
        final Headers h3 = headersWith("id", "3");

        store.put(windowed("k", 100L, 150L), AggregationWithHeaders.make("a1", h1));
        store.put(windowed("k", 200L, 250L), AggregationWithHeaders.make("a2", h2));
        store.put(windowed("k", 300L, 350L), AggregationWithHeaders.make("a3", h3));

        final List<KeyValue<Long, Headers>> collected = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it = store.fetch("k")) {
            while (it.hasNext()) {
                final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = it.next();
                collected.add(KeyValue.pair(next.key.window().start(), next.value.headers()));
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
    public void shouldPreserveHeadersAcrossFindSessions() {
        final Headers h1 = headersWith("id", "early");
        final Headers h2 = headersWith("id", "late");

        store.put(windowed("k", 100L, 150L), AggregationWithHeaders.make("a1", h1));
        store.put(windowed("k", 200L, 250L), AggregationWithHeaders.make("a2", h2));

        final List<KeyValue<Long, Headers>> collected = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it =
                 store.findSessions("k", 100L, 250L)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = it.next();
                collected.add(KeyValue.pair(next.key.window().start(), next.value.headers()));
            }
        }

        assertEquals(2, collected.size());
        assertEquals(100L, collected.get(0).key.longValue());
        assertEquals(h1, collected.get(0).value);
        assertEquals(200L, collected.get(1).key.longValue());
        assertEquals(h2, collected.get(1).value);
    }

    @Test
    public void shouldPreserveHeadersAcrossBackwardFindSessions() {
        final Headers h1 = headersWith("id", "early");
        final Headers h2 = headersWith("id", "late");

        store.put(windowed("k", 100L, 150L), AggregationWithHeaders.make("a1", h1));
        store.put(windowed("k", 200L, 250L), AggregationWithHeaders.make("a2", h2));

        final List<KeyValue<Long, Headers>> collected = new ArrayList<>();
        try (KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it =
                 store.backwardFindSessions("k", 100L, 250L)) {
            while (it.hasNext()) {
                final KeyValue<Windowed<String>, AggregationWithHeaders<String>> next = it.next();
                collected.add(KeyValue.pair(next.key.window().start(), next.value.headers()));
            }
        }

        assertEquals(2, collected.size());
        assertEquals(200L, collected.get(0).key.longValue());
        assertEquals(h2, collected.get(0).value);
        assertEquals(100L, collected.get(1).key.longValue());
        assertEquals(h1, collected.get(1).value);
    }

    @Test
    public void shouldReturnEmptyIteratorForMissingKey() {
        try (KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it = store.fetch("missing")) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void shouldPreserveEmptyHeaders() {
        final Windowed<String> key = windowed("k", 100L, 200L);
        store.put(key, AggregationWithHeaders.make("agg", new RecordHeaders()));

        final AggregationWithHeaders<String> result = store.fetchSession("k", 100L, 200L);
        assertEquals("agg", result.aggregation());
        assertEquals(new RecordHeaders(), result.headers());
        assertTrue(result.headers().toArray().length == 0);
    }

    private static Windowed<String> windowed(final String key, final long start, final long end) {
        return new Windowed<>(key, new SessionWindow(start, end));
    }

    private static Headers headersWith(final String key, final String value) {
        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }
}
