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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Direct coverage of {@link CachingKeyValueStoreWithHeaders} (KAFKA-20328). The class adds no overrides to
 * {@link CachingKeyValueStore}; its contract is that serialized {@link ValueTimestampHeaders} bytes pass
 * through the cache unchanged (cache hit, flush to the wrapped store, flush listener), leaving header-aware
 * deserialization to the metered layer. This pins that contract against a real {@link ThreadCache} and
 * {@link InMemoryKeyValueStore}.
 */
public class CachingKeyValueStoreWithHeadersTest {

    private static final String TOPIC = "topic";
    private static final Bytes KEY = Bytes.wrap("key".getBytes());

    private final ValueTimestampHeadersSerde<String> valueSerde = new ValueTimestampHeadersSerde<>(Serdes.String());
    private final ValueTimestampHeaders<String> value =
        ValueTimestampHeaders.make("value", 42L, new RecordHeaders().add("h", "v".getBytes()));
    private final byte[] serializedValue = valueSerde.serializer().serialize(TOPIC, value);

    private KeyValueStore<Bytes, byte[]> underlying;
    private CachingKeyValueStoreWithHeaders store;
    private CacheFlushListenerStub<String, ValueTimestampHeaders<String>> flushListener;

    @BeforeEach
    public void setUp() {
        underlying = new InMemoryKeyValueStore("store");
        store = new CachingKeyValueStoreWithHeaders(underlying);
        flushListener = new CacheFlushListenerStub<>(new StringDeserializer(), valueSerde.deserializer());
        store.setFlushListener(flushListener, false);
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 10_000L, new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(null, null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, TOPIC, new RecordHeaders()));
        store.init(context, store);
    }

    @AfterEach
    public void tearDown() {
        store.close();
    }

    // CachingKeyValueStore behavior (cache hits, eviction, IQv2) is covered by CachingInMemoryKeyValueStoreTest;
    // this single round trip pins the headers-specific contract: header-format bytes are opaque to the cache.
    @Test
    public void shouldRoundTripHeaderFormatBytesThroughCacheUnchanged() {
        store.put(KEY, serializedValue);

        assertNull(underlying.get(KEY), "write should still be in the cache only");
        assertArrayEquals(serializedValue, store.get(KEY));

        store.flushCache();

        assertArrayEquals(serializedValue, underlying.get(KEY));
        assertEquals(value, flushListener.forwarded.get("key").newValue);
        assertNull(flushListener.forwarded.get("key").oldValue);
    }
}
