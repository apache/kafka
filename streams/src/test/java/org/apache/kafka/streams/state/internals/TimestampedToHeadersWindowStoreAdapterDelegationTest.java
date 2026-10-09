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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Method-level coverage of {@link TimestampedToHeadersWindowStoreAdapter} over a mocked inner store: reads
 * convert/wrap, put strips headers. Strict stubs make a call to the wrong inner overload fail the test
 * instead of returning {@code null}. See {@link TimestampedToHeadersWindowStoreAdapterCompletenessTest}
 * for the completeness guard and {@link TimestampedToHeadersWindowStoreAdapterTest} for coverage against a
 * real RocksDB store.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedToHeadersWindowStoreAdapterDelegationTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes());
    private static final Bytes KEY_TO = Bytes.wrap("key-to".getBytes());
    private static final Windowed<Bytes> WINDOWED_KEY = new Windowed<>(KEY, new TimeWindow(0L, 10L));
    private static final long WINDOW_START = 5L;
    // [8-byte timestamp][value], the format of the timestamped inner store
    private static final byte[] RAW_VALUE = new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
    private static final Instant T_FROM = Instant.ofEpochMilli(0L);
    private static final Instant T_TO = Instant.ofEpochMilli(10L);

    @Mock(extraInterfaces = TimestampedBytesStore.class)
    private WindowStore<Bytes, byte[]> inner;

    private TimestampedToHeadersWindowStoreAdapter adapter;

    @BeforeEach
    public void setUp() {
        when(inner.persistent()).thenReturn(true);
        adapter = new TimestampedToHeadersWindowStoreAdapter(inner);
    }

    @Test
    public void shouldStripHeadersOnPut() {
        adapter.put(KEY, convertToHeaderFormat(RAW_VALUE), WINDOW_START);
        verify(inner).put(KEY, RAW_VALUE, WINDOW_START);
    }

    @Test
    public void shouldConvertFetchByKeyAndTimestampToHeaderFormat() {
        when(inner.fetch(KEY, WINDOW_START)).thenReturn(RAW_VALUE);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), adapter.fetch(KEY, WINDOW_START));
    }

    @Test
    public void shouldReturnNullWhenFetchByKeyAndTimestampReturnsNull() {
        when(inner.fetch(KEY, WINDOW_START)).thenReturn(null);
        assertNull(adapter.fetch(KEY, WINDOW_START));
    }

    // Each call is applied to the inner store to stub it and to the adapter to exercise it, so the
    // adapter must forward to the same overload with the same arguments.
    private static Stream<Arguments> windowStoreIteratorCalls() {
        return Stream.of(
            call("fetch(key, long, long)", s -> s.fetch(KEY, 0L, 10L)),
            call("fetch(key, Instant, Instant)", s -> s.fetch(KEY, T_FROM, T_TO)),
            call("backwardFetch(key, long, long)", s -> s.backwardFetch(KEY, 0L, 10L)),
            call("backwardFetch(key, Instant, Instant)", s -> s.backwardFetch(KEY, T_FROM, T_TO))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("windowStoreIteratorCalls")
    public void shouldConvertWindowStoreIteratorValues(
        final Function<WindowStore<Bytes, byte[]>, WindowStoreIterator<byte[]>> call) {
        @SuppressWarnings("unchecked")
        final WindowStoreIterator<byte[]> innerIterator = mock(WindowStoreIterator.class);
        when(innerIterator.next()).thenReturn(KeyValue.pair(WINDOW_START, RAW_VALUE));
        when(call.apply(inner)).thenReturn(innerIterator);

        final KeyValue<Long, byte[]> next = call.apply(adapter).next();

        assertEquals(WINDOW_START, next.key);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), next.value);
    }

    private static Stream<Arguments> keyValueIteratorCalls() {
        return Stream.of(
            call("fetch(keyFrom, keyTo, long, long)", s -> s.fetch(KEY, KEY_TO, 0L, 10L)),
            call("fetch(keyFrom, keyTo, Instant, Instant)", s -> s.fetch(KEY, KEY_TO, T_FROM, T_TO)),
            call("backwardFetch(keyFrom, keyTo, long, long)", s -> s.backwardFetch(KEY, KEY_TO, 0L, 10L)),
            call("backwardFetch(keyFrom, keyTo, Instant, Instant)", s -> s.backwardFetch(KEY, KEY_TO, T_FROM, T_TO)),
            call("fetchAll(long, long)", s -> s.fetchAll(0L, 10L)),
            call("fetchAll(Instant, Instant)", s -> s.fetchAll(T_FROM, T_TO)),
            call("backwardFetchAll(long, long)", s -> s.backwardFetchAll(0L, 10L)),
            call("backwardFetchAll(Instant, Instant)", s -> s.backwardFetchAll(T_FROM, T_TO)),
            call("all()", WindowStore::all),
            call("backwardAll()", WindowStore::backwardAll)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keyValueIteratorCalls")
    public void shouldConvertKeyValueIteratorValues(
        final Function<WindowStore<Bytes, byte[]>, KeyValueIterator<Windowed<Bytes>, byte[]>> call) {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIterator = mock(KeyValueIterator.class);
        when(innerIterator.next()).thenReturn(KeyValue.pair(WINDOWED_KEY, RAW_VALUE));
        when(call.apply(inner)).thenReturn(innerIterator);

        final KeyValue<Windowed<Bytes>, byte[]> next = call.apply(adapter).next();

        assertEquals(WINDOWED_KEY, next.key);
        assertArrayEquals(convertToHeaderFormat(RAW_VALUE), next.value);
    }

    @Test
    public void shouldDelegateName() {
        when(inner.name()).thenReturn("inner-store");
        assertEquals("inner-store", adapter.name());
    }

    @Test
    public void shouldDelegateIsOpen() {
        when(inner.isOpen()).thenReturn(true);
        assertTrue(adapter.isOpen());
    }

    @Test
    public void shouldReturnPersistentTrue() {
        assertTrue(adapter.persistent());
    }

    private static <R> Arguments call(final String name, final Function<WindowStore<Bytes, byte[]>, R> call) {
        return Arguments.of(Named.of(name, call));
    }
}
