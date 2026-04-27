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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ChangeLoggingListValueBytesStoreWithHeadersTest {

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore root = new InMemoryKeyValueStore("kv");
    private final ListValueStore listStore = new ListValueStore(root);
    private final ChangeLoggingListValueBytesStoreWithHeaders store =
        new ChangeLoggingListValueBytesStoreWithHeaders(listStore);

    private final Bytes key = Bytes.wrap("key".getBytes());
    private final ValueTimestampHeadersSerializer<byte[]> serializer =
        new ValueTimestampHeadersSerializer<>(Serdes.ByteArray().serializer());

    private byte[] entryWithHeaders(final byte[] value, final long ts, final Headers h) {
        return serializer.serialize("topic", ValueTimestampHeaders.make(value, ts, h));
    }

    @BeforeEach
    public void before() {
        final InternalMockProcessorContext<String, Long> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        context.setTime(0);
        store.init(context, store);
    }

    @AfterEach
    public void after() {
        store.close();
    }

    @Test
    public void shouldLogFullListAndExtractHeadersFromNewEntryOnPut() {
        final RecordHeaders h1 = new RecordHeaders();
        h1.add(new RecordHeader("k1", "v1".getBytes()));

        final byte[] entry = entryWithHeaders("first".getBytes(), 100L, h1);
        store.put(key, entry);

        assertEquals(1, collector.collected().size());
        assertEquals(key, collector.collected().get(0).key());
        // The logged value is the full list bytes from the inner ListValueStore.
        // It is non-null (the list has one element) and equals what's in the inner store.
        assertNotNull(collector.collected().get(0).value());
        assertArrayEquals(root.get(key), (byte[]) collector.collected().get(0).value());
        // Timestamp and headers come from the newly-appended entry's serialized bytes.
        assertEquals(100L, collector.collected().get(0).timestamp());
        final Headers logged = collector.collected().get(0).headers();
        assertEquals(1, logged.toArray().length);
        assertEquals("v1", new String(logged.lastHeader("k1").value()));
    }

    @Test
    public void shouldUseLatestEntryHeadersOnSecondPut() {
        final RecordHeaders h1 = new RecordHeaders();
        h1.add(new RecordHeader("k1", "v1".getBytes()));
        final RecordHeaders h2 = new RecordHeaders();
        h2.add(new RecordHeader("k2", "v2".getBytes()));

        store.put(key, entryWithHeaders("first".getBytes(), 100L, h1));
        store.put(key, entryWithHeaders("second".getBytes(), 200L, h2));

        assertEquals(2, collector.collected().size());

        // Second log uses the second entry's timestamp/headers
        assertEquals(200L, collector.collected().get(1).timestamp());
        final Headers logged = collector.collected().get(1).headers();
        assertEquals(1, logged.toArray().length);
        assertEquals("v2", new String(logged.lastHeader("k2").value()));
        // Inner store now holds a list of two elements; logged value is that full list.
        assertArrayEquals(root.get(key), (byte[]) collector.collected().get(1).value());
    }

    @Test
    public void shouldLogNullAndCurrentRecordContextOnTombstone() {
        // First populate, then tombstone with a known record context
        store.put(key, entryWithHeaders("first".getBytes(), 100L, new RecordHeaders()));

        final InternalMockProcessorContext<String, Long> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        context.setTime(555L);
        context.headers().add("ctx", "ctxValue".getBytes());
        store.init(context, store);

        store.put(key, null);

        // Most recent log entry is the tombstone: value=null with the current context's ts/headers
        final int last = collector.collected().size() - 1;
        assertEquals(key, collector.collected().get(last).key());
        assertNull(collector.collected().get(last).value());
        assertEquals(555L, collector.collected().get(last).timestamp());
        final Headers tombstoneHeaders = collector.collected().get(last).headers();
        assertEquals(1, tombstoneHeaders.toArray().length);
        assertEquals("ctxValue", new String(tombstoneHeaders.lastHeader("ctx").value()));

        // Inner store has key removed
        assertNull(root.get(key));
    }

    @Test
    public void shouldHandleEmptyHeadersOnPut() {
        final byte[] entry = entryWithHeaders("v".getBytes(), 50L, new RecordHeaders());
        store.put(key, entry);

        assertEquals(1, collector.collected().size());
        assertEquals(50L, collector.collected().get(0).timestamp());
        assertEquals(0, collector.collected().get(0).headers().toArray().length);
    }
}
