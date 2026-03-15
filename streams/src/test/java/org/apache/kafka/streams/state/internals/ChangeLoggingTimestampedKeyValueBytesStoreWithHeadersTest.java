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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangeLoggingTimestampedKeyValueBytesStoreWithHeadersTest {

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore root = new InMemoryKeyValueStore("kv");
    private final ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders store =
        new ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders(root);
    private final Bytes hi = Bytes.wrap("hi".getBytes());
    private final Bytes hello = Bytes.wrap("hello".getBytes());

    private final RecordHeaders thereHeaders = new RecordHeaders();
    private final RecordHeaders worldHeaders = new RecordHeaders();

    private final ValueTimestampHeaders<byte[]> there = ValueTimestampHeaders.make("there".getBytes(), 97L, thereHeaders);
    private final ValueTimestampHeaders<byte[]> world = ValueTimestampHeaders.make("world".getBytes(), 98L, worldHeaders);

    private byte[] rawThere;
    private byte[] rawWorld;

    @BeforeEach
    public void before() {
        thereHeaders.add("key1", "value1".getBytes());
        worldHeaders.add("key2", "value2".getBytes());

        final ValueTimestampHeadersSerializer<byte[]> serializer =
            new ValueTimestampHeadersSerializer<>(Serdes.ByteArray().serializer());
        rawThere = serializer.serialize("topic", there);
        rawWorld = serializer.serialize("topic", world);

        final InternalMockProcessorContext<String, Long> context = mockContext();
        context.setTime(0);
        store.init(context, store);
    }

    private InternalMockProcessorContext<String, Long> mockContext() {
        return new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
    }

    @AfterEach
    public void after() {
        store.close();
    }

    @Test
    public void shouldDelegateInit() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        final KeyValueStore<Bytes, byte[]> inner = mock(InMemoryKeyValueStore.class);
        final StateStore outer = new ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders(inner);

        outer.init(context, outer);
        verify(inner).init(context, outer);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, rawThere);

        assertEquals(rawThere, root.get(hi));
        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(97L, collector.collected().get(0).timestamp());

        // Verify headers are logged
        final Headers loggedHeaders = collector.collected().get(0).headers();
        assertEquals(1, loggedHeaders.toArray().length);
        assertEquals("value1", new String(loggedHeaders.lastHeader("key1").value()));
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));
        assertEquals(rawThere, root.get(hi));
        assertEquals(rawWorld, root.get(hello));
    }

    @Test
    public void shouldLogChangesOnPutAll() {
        store.putAll(Arrays.asList(KeyValue.pair(hi, rawThere),
                                   KeyValue.pair(hello, rawWorld)));

        assertEquals(2, collector.collected().size());

        // First entry
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(97L, collector.collected().get(0).timestamp());
        final Headers headers0 = collector.collected().get(0).headers();
        assertEquals(1, headers0.toArray().length);
        assertEquals("value1", new String(headers0.lastHeader("key1").value()));

        // Second entry
        assertEquals(hello, collector.collected().get(1).key());
        assertArrayEquals(world.value(), (byte[]) collector.collected().get(1).value());
        assertEquals(98L, collector.collected().get(1).timestamp());
        final Headers headers1 = collector.collected().get(1).headers();
        assertEquals(1, headers1.toArray().length);
        assertEquals("value2", new String(headers1.lastHeader("key2").value()));
    }

    @Test
    public void shouldPropagateDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        assertEquals(0L, root.approximateNumEntries());
        assertNull(root.get(hi));
    }

    @Test
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, rawThere);
        assertEquals(rawThere, store.delete(hi));
    }

    @Test
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, rawThere);
        store.delete(hi);

        assertEquals(2, collector.collected().size());

        // First record is the put
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(97L, collector.collected().get(0).timestamp());
        final Headers headers0 = collector.collected().get(0).headers();
        assertEquals(1, headers0.toArray().length);
        assertEquals("value1", new String(headers0.lastHeader("key1").value()));

        // Second record is the delete
        assertEquals(hi, collector.collected().get(1).key());
        assertNull(collector.collected().get(1).value());
        assertEquals(0L, collector.collected().get(1).timestamp());
        assertEquals(0, collector.collected().get(1).headers().toArray().length);
    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        assertEquals(rawThere, root.get(hi));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        assertEquals(rawThere, root.get(hi));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(97L, collector.collected().get(0).timestamp());

        final Headers headers = collector.collected().get(0).headers();
        assertEquals(1, headers.toArray().length);
        assertEquals("value1", new String(headers.lastHeader("key1").value()));
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(there.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(97L, collector.collected().get(0).timestamp());
        final Headers headers0 = collector.collected().get(0).headers();
        assertEquals(1, headers0.toArray().length);
        assertEquals("value1", new String(headers0.lastHeader("key1").value()));
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, rawThere);
        assertEquals(rawThere, store.putIfAbsent(hi, rawWorld));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        assertNull(store.putIfAbsent(hi, rawThere));
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, rawWorld);
        assertEquals(rawWorld, store.get(hello));
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() {
        assertNull(store.get(hello));
    }

    @Test
    public void shouldHandleNullValueInPut() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        context.setTime(42L);
        context.headers().add("headerKey", "headerValue".getBytes());
        store.init(context, store);

        store.put(hi, null);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertNull(collector.collected().get(0).value());
        assertEquals(42L, collector.collected().get(0).timestamp());
        assertEquals(1, collector.collected().get(0).headers().toArray().length);
        assertEquals("headerKey", collector.collected().get(0).headers().toArray()[0].key());
        assertArrayEquals("headerValue".getBytes(), collector.collected().get(0).headers().toArray()[0].value());
    }

    @Test
    public void shouldHandleNullValueInPutIfAbsent() {
        final InternalMockProcessorContext<String, Long> context = mockContext();
        context.setTime(50L);
        context.headers().add("headerKey", "headerValue".getBytes());
        store.init(context, store);

        store.putIfAbsent(hi, null);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertNull(collector.collected().get(0).value());
        assertEquals(50L, collector.collected().get(0).timestamp());
        assertEquals(1, collector.collected().get(0).headers().toArray().length);
        assertEquals("headerKey", collector.collected().get(0).headers().toArray()[0].key());
        assertArrayEquals("headerValue".getBytes(), collector.collected().get(0).headers().toArray()[0].value());
    }

    @Test
    public void shouldHandleEmptyHeaders() {
        final RecordHeaders emptyHeaders = new RecordHeaders();
        final ValueTimestampHeaders<byte[]> valueWithEmptyHeaders =
            ValueTimestampHeaders.make("test".getBytes(), 100L, emptyHeaders);

        final ValueTimestampHeadersSerializer<byte[]> serializer =
            new ValueTimestampHeadersSerializer<>(Serdes.ByteArray().serializer());
        final byte[] rawValueWithEmptyHeaders = serializer.serialize("topic", valueWithEmptyHeaders);

        store.put(hi, rawValueWithEmptyHeaders);

        assertEquals(1, collector.collected().size());
        assertEquals(hi, collector.collected().get(0).key());
        assertArrayEquals(valueWithEmptyHeaders.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(100L, collector.collected().get(0).timestamp());

        // Verify empty headers
        final Headers loggedHeaders = collector.collected().get(0).headers();
        assertEquals(0, loggedHeaders.toArray().length);
    }

    @Test
    public void shouldHandleMultipleHeadersInSingleRecord() {
        final RecordHeaders multiHeaders = new RecordHeaders();
        multiHeaders.add("header1", "value1".getBytes());
        multiHeaders.add("header2", "value2".getBytes());
        multiHeaders.add("header3", "value3".getBytes());

        final ValueTimestampHeaders<byte[]> valueWithMultiHeaders =
            ValueTimestampHeaders.make("multi".getBytes(), 123L, multiHeaders);

        final ValueTimestampHeadersSerializer<byte[]> serializer =
            new ValueTimestampHeadersSerializer<>(Serdes.ByteArray().serializer());
        final byte[] rawValueWithMultiHeaders = serializer.serialize("topic", valueWithMultiHeaders);

        store.put(hello, rawValueWithMultiHeaders);

        assertEquals(1, collector.collected().size());
        assertEquals(hello, collector.collected().get(0).key());
        assertArrayEquals(valueWithMultiHeaders.value(), (byte[]) collector.collected().get(0).value());
        assertEquals(123L, collector.collected().get(0).timestamp());

        // Verify multiple headers
        final Headers loggedHeaders = collector.collected().get(0).headers();
        assertEquals(3, loggedHeaders.toArray().length);
        assertEquals("value1", new String(loggedHeaders.lastHeader("header1").value()));
        assertEquals("value2", new String(loggedHeaders.lastHeader("header2").value()));
        assertEquals("value3", new String(loggedHeaders.lastHeader("header3").value()));
    }
}