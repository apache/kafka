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
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedToHeadersStoreAdapterTest {

    // Timestamped format: an 8-byte timestamp prefix followed by the raw value.
    private static final byte[] TIMESTAMPED_VALUE = {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
    private static final byte[] OLD_TIMESTAMPED_VALUE = {0, 0, 0, 0, 0, 0, 0, 41, 'o', 'l', 'd'};

    @Mock(extraInterfaces = TimestampedBytesStore.class)
    private KeyValueStore<Bytes, byte[]> mockStore;

    @Mock
    private KeyValueIterator<Bytes, byte[]> mockIterator;

    private TimestampedToHeadersStoreAdapter adapter;

    private TimestampedToHeadersStoreAdapter createAdapter() {
        when(mockStore.persistent()).thenReturn(true);
        return new TimestampedToHeadersStoreAdapter(mockStore);
    }

    private void assertConvertsTimestampedToHeaders(final KeyValueIterator<Bytes, byte[]> result) {
        final Bytes key = new Bytes("k".getBytes());
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(KeyValue.pair(key, TIMESTAMPED_VALUE));

        assertTrue(result.hasNext());
        final KeyValue<Bytes, byte[]> entry = result.next();
        assertEquals(key, entry.key);
        // Timestamped format only prepends empty headers; the plain conversion would also insert
        // an 8-byte timestamp, so this array comparison proves timestampedToHeaders was wired.
        assertArrayEquals(convertToHeaderFormat(TIMESTAMPED_VALUE), entry.value);
    }

    @Test
    public void shouldThrowIfStoreIsNotPersistent() {
        when(mockStore.persistent()).thenReturn(false);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(mockStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a persistent store"));
    }

    @Test
    public void shouldThrowIfStoreIsNotTimestamped() {
        @SuppressWarnings("unchecked")
        final KeyValueStore<Bytes, byte[]> plainStore = mock(KeyValueStore.class);
        when(plainStore.persistent()).thenReturn(true);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(plainStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a timestamped store"));
    }

    @Test
    public void shouldPutRawTimestampedValueToStore() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] valueWithHeaders = convertToHeaderFormat(TIMESTAMPED_VALUE);

        adapter.put(key, valueWithHeaders);

        verify(mockStore).put(eq(key), eq(TIMESTAMPED_VALUE));
    }

    @Test
    public void shouldGetAndConvertToHeaderFormat() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        when(mockStore.get(key)).thenReturn(TIMESTAMPED_VALUE);

        final byte[] result = adapter.get(key);

        assertArrayEquals(convertToHeaderFormat(TIMESTAMPED_VALUE), result);
    }

    @Test
    public void shouldReturnNullWhenStoreReturnsNull() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        when(mockStore.get(key)).thenReturn(null);

        final byte[] result = adapter.get(key);

        assertNull(result);
    }

    @Test
    public void shouldPutIfAbsentAndConvertResult() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] valueWithHeaders = convertToHeaderFormat(TIMESTAMPED_VALUE);
        when(mockStore.putIfAbsent(eq(key), eq(TIMESTAMPED_VALUE))).thenReturn(OLD_TIMESTAMPED_VALUE);

        final byte[] result = adapter.putIfAbsent(key, valueWithHeaders);

        assertArrayEquals(convertToHeaderFormat(OLD_TIMESTAMPED_VALUE), result);
    }

    @Test
    public void shouldDeleteAndConvertResult() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        when(mockStore.delete(key)).thenReturn(OLD_TIMESTAMPED_VALUE);

        final byte[] result = adapter.delete(key);

        assertArrayEquals(convertToHeaderFormat(OLD_TIMESTAMPED_VALUE), result);
    }

    @Test
    public void shouldPutAllEntries() {
        adapter = createAdapter();
        final Bytes key1 = new Bytes("key1".getBytes());
        final Bytes key2 = new Bytes("key2".getBytes());
        final List<KeyValue<Bytes, byte[]>> entries = Arrays.asList(
            KeyValue.pair(key1, convertToHeaderFormat(TIMESTAMPED_VALUE)),
            KeyValue.pair(key2, convertToHeaderFormat(OLD_TIMESTAMPED_VALUE))
        );

        adapter.putAll(entries);

        verify(mockStore).put(eq(key1), eq(TIMESTAMPED_VALUE));
        verify(mockStore).put(eq(key2), eq(OLD_TIMESTAMPED_VALUE));
    }

    @Test
    public void shouldWrapRangeIterator() {
        adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.range(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.range(from, to);

        assertNotNull(result);
        assertConvertsTimestampedToHeaders(result);
    }

    @Test
    public void shouldWrapReverseRangeIterator() {
        adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.reverseRange(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseRange(from, to);

        assertNotNull(result);
        assertConvertsTimestampedToHeaders(result);
    }

    @Test
    public void shouldWrapAllIterator() {
        adapter = createAdapter();
        when(mockStore.all()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.all();

        assertNotNull(result);
        assertConvertsTimestampedToHeaders(result);
    }

    @Test
    public void shouldWrapReverseAllIterator() {
        adapter = createAdapter();
        when(mockStore.reverseAll()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseAll();

        assertNotNull(result);
        assertConvertsTimestampedToHeaders(result);
    }

    @Test
    public void shouldWrapPrefixScanIterator() {
        adapter = createAdapter();
        when(mockStore.prefixScan(any(), any())).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.prefixScan("prefix", (topic, data) -> data.getBytes());

        assertNotNull(result);
        assertConvertsTimestampedToHeaders(result);
    }

    @Test
    public void shouldHandleKeyQuery() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] timestampedValue = "test-value".getBytes();
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(timestampedValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertArrayEquals(convertToHeaderFormat(timestampedValue), result.getResult());
    }

    @Test
    public void shouldHandleKeyQueryWithNullResult() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(null);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertNull(result.getResult());
    }

    @Test
    public void shouldHandleFailedKeyQuery() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forUnknownQueryType(query, mockStore);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertFalse(result.isSuccess());
    }

    @Test
    public void shouldHandleRangeQuery() {
        adapter = createAdapter();
        final RangeQuery<Bytes, byte[]> query = RangeQuery.withRange(
            new Bytes("a".getBytes()),
            new Bytes("z".getBytes())
        );

        final QueryResult<KeyValueIterator<Bytes, byte[]>> mockResult = QueryResult.forResult(mockIterator);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<KeyValueIterator<Bytes, byte[]>> result = adapter.query(
            query,
            PositionBound.unbounded(),
            new QueryConfig(false)
        );

        assertTrue(result.isSuccess());
        assertNotNull(result.getResult());
        assertConvertsTimestampedToHeaders(result.getResult());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateOtherQueryTypesToStore() {
        adapter = createAdapter();
        // Any query that is neither KeyQuery nor RangeQuery falls through to the
        // else branch and is passed straight to the underlying store, unchanged.
        final Query<String> query = mock(Query.class);
        final QueryResult<String> mockResult = QueryResult.forResult("delegated");
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<String> result =
            adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertSame(mockResult, result);
        assertTrue(result.isSuccess());
        assertEquals("delegated", result.getResult());
    }

    @Test
    public void shouldCollectExecutionInfoForKeyQuery() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] timestampedValue = "test-value".getBytes();
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(timestampedValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(true));

        assertTrue(result.isSuccess());
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        final String executionInfo = String.join("\n", result.getExecutionInfo());
        assertTrue(executionInfo.contains("Handled in"), "Expected execution info to contain handling information");
        assertTrue(executionInfo.contains(TimestampedToHeadersStoreAdapter.class.getName()),
            "Expected execution info to mention TimestampedToHeadersStoreAdapter");
    }

    @Test
    public void shouldDelegateName() {
        when(mockStore.name()).thenReturn("test-store");
        adapter = createAdapter();

        assertEquals("test-store", adapter.name());
    }

    @Test
    public void shouldReturnTrueForPersistent() {
        adapter = createAdapter();

        assertTrue(adapter.persistent());
    }

    @Test
    public void shouldDelegateIsOpen() {
        when(mockStore.isOpen()).thenReturn(true);
        adapter = createAdapter();

        assertTrue(adapter.isOpen());
    }

    @Test
    public void shouldDelegateApproximateNumEntries() {
        when(mockStore.approximateNumEntries()).thenReturn(42L);
        adapter = createAdapter();

        assertEquals(42L, adapter.approximateNumEntries());
    }

    @Test
    public void shouldDelegateGetPosition() {
        adapter = createAdapter();
        when(mockStore.getPosition()).thenReturn(null);

        adapter.getPosition();

        verify(mockStore).getPosition();
    }
}
