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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Direct coverage of {@link MeteredWindowedKeyValueIterator} (KAFKA-20328): key/value deserialization and the
 * open-iterator and latency bookkeeping that {@link MeteredWindowedKeyValueWithHeadersIterator} inherits.
 * Header-specific behavior is covered by {@link MeteredWindowedKeyValueWithHeadersIteratorTest}.
 */
public class MeteredWindowedKeyValueIteratorTest {

    private static final Windowed<Bytes> RAW_KEY = new Windowed<>(Bytes.wrap("key".getBytes()), new TimeWindow(0L, 10L));
    private static final byte[] RAW_VALUE = "value".getBytes();

    private final MockTime time = new MockTime();
    private final Sensor operationSensor = mock(Sensor.class);
    private final Sensor iteratorSensor = mock(Sensor.class);
    private final LongAdder numOpenIterators = new LongAdder();
    private final Set<MeteredIterator> openIterators = new HashSet<>();

    private KeyValueIterator<Windowed<Bytes>, byte[]> inner;
    private MeteredWindowedKeyValueIterator<String, String> iterator;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        inner = mock(KeyValueIterator.class);
        iterator = new MeteredWindowedKeyValueIterator<>(
            inner,
            operationSensor,
            iteratorSensor,
            String::new,
            String::new,
            time,
            numOpenIterators,
            openIterators
        );
    }

    @Test
    public void shouldDeserializeKeyAndValueOnNext() {
        when(inner.next()).thenReturn(KeyValue.pair(RAW_KEY, RAW_VALUE));

        assertEquals(KeyValue.pair(new Windowed<>("key", RAW_KEY.window()), "value"), iterator.next());
    }

    @Test
    public void shouldDeserializeKeyOnPeekNextKey() {
        when(inner.peekNextKey()).thenReturn(RAW_KEY);

        assertEquals(new Windowed<>("key", RAW_KEY.window()), iterator.peekNextKey());
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
