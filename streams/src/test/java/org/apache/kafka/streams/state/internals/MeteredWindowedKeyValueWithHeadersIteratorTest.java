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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Direct coverage of {@link MeteredWindowedKeyValueWithHeadersIterator} (KAFKA-20328). Unlike its
 * {@link MeteredWindowedKeyValueIterator} base, {@code next()} deserializes the value first and passes the
 * headers carried by that value to the key deserializer, so a header-aware key serde sees the record's
 * headers; the value is then converted (e.g. to the plain value or a {@code ReadOnlyRecord}). The iterator
 * also inherits the base's open-iterator and latency bookkeeping on close.
 */
public class MeteredWindowedKeyValueWithHeadersIteratorTest {

    private static final Windowed<Bytes> RAW_KEY = new Windowed<>(Bytes.wrap("key".getBytes()), new TimeWindow(0L, 10L));
    private static final byte[] RAW_VALUE = "raw-value".getBytes();

    private final MockTime time = new MockTime();
    private final Sensor operationSensor = mock(Sensor.class);
    private final Sensor iteratorSensor = mock(Sensor.class);
    private final LongAdder numOpenIterators = new LongAdder();
    private final Set<MeteredIterator> openIterators = new HashSet<>();
    private final Headers valueHeaders = new RecordHeaders().add("h", "v".getBytes());
    private final ValueTimestampHeaders<String> deserializedValue = ValueTimestampHeaders.make("value", 42L, valueHeaders);
    private final List<byte[]> deserializedValueBytes = new ArrayList<>();
    private final List<Headers> keyDeserializerHeaders = new ArrayList<>();

    private KeyValueIterator<Windowed<Bytes>, byte[]> inner;
    private MeteredWindowedKeyValueWithHeadersIterator<String, ValueTimestampHeaders<String>, String> iterator;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        inner = mock(KeyValueIterator.class);
        iterator = new MeteredWindowedKeyValueWithHeadersIterator<>(
            inner,
            operationSensor,
            iteratorSensor,
            bytes -> {
                deserializedValueBytes.add(bytes);
                return deserializedValue;
            },
            (bytes, headers) -> {
                keyDeserializerHeaders.add(headers);
                return new String(bytes);
            },
            ValueTimestampHeaders::headers,
            ValueTimestampHeaders::value,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Test
    public void shouldDeserializeKeyWithHeadersFromDeserializedValue() {
        when(inner.next()).thenReturn(KeyValue.pair(RAW_KEY, RAW_VALUE));

        final KeyValue<Windowed<String>, String> next = iterator.next();

        assertEquals(new Windowed<>("key", RAW_KEY.window()), next.key);
        assertEquals("value", next.value);
        // the value bytes are deserialized exactly once, and those headers reach the key deserializer
        assertEquals(1, deserializedValueBytes.size());
        assertSame(RAW_VALUE, deserializedValueBytes.get(0));
        assertEquals(1, keyDeserializerHeaders.size());
        assertSame(valueHeaders, keyDeserializerHeaders.get(0));
    }

    @Test
    public void shouldDelegateHasNext() {
        when(inner.hasNext()).thenReturn(true, false);
        assertTrue(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldRegisterAsOpenIteratorOnCreation() {
        assertEquals(1L, numOpenIterators.sum());
        assertTrue(openIterators.contains(iterator));
        assertEquals(time.milliseconds(), iterator.startTimestamp());
    }

    @Test
    public void shouldCloseInnerRecordLatencyAndDeregisterOnClose() {
        time.sleep(5L);

        iterator.close();

        verify(inner).close();
        verify(operationSensor).record(5_000_000.0);
        verify(iteratorSensor).record(5_000_000.0);
        assertEquals(0L, numOpenIterators.sum());
        assertFalse(openIterators.contains(iterator));
    }

    @Test
    public void shouldRecordLatencyAndDeregisterEvenIfInnerCloseThrows() {
        doThrow(new IllegalStateException("boom")).when(inner).close();
        time.sleep(5L);

        assertThrows(IllegalStateException.class, iterator::close);

        verify(operationSensor).record(5_000_000.0);
        verify(iteratorSensor).record(5_000_000.0);
        assertEquals(0L, numOpenIterators.sum());
        assertFalse(openIterators.contains(iterator));
    }
}
