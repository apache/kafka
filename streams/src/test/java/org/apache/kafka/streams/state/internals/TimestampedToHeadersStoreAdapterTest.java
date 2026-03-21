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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedToHeadersStoreAdapterTest {

    @Mock
    private TimestampedAndPersistentStore mockStore;

    @Mock
    private KeyValueIterator<Bytes, byte[]> mockIterator;

    private TimestampedToHeadersStoreAdapter createAdapter() {
        when(mockStore.persistent()).thenReturn(true);
        return new TimestampedToHeadersStoreAdapter(mockStore);
    }

    private static byte[] headerFormatValue(final byte[] timestampedValue) {
        return convertToHeaderFormat(timestampedValue);
    }

    private static byte[] timestampedValue(final long timestamp, final byte[] value) {
        final byte[] result = new byte[8 + value.length];
        long ts = timestamp;
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (ts & 0xFF);
            ts >>>= 8;
        }
        System.arraycopy(value, 0, result, 8, value.length);
        return result;
    }

    @Test
    public void shouldThrowIfStoreIsNotPersistent() {
        final KeyValueStore<Bytes, byte[]> nonPersistent = new InMemoryKeyValueStore("test");

        assertThrows(IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(nonPersistent));
    }

    @Test
    public void shouldThrowIfStoreIsNotTimestamped() {
        final RocksDBStore plainStore = new RocksDBStore("test", "scope");

        assertThrows(IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(plainStore));
    }

    @Test
    public void shouldPutByStrippingHeadersPrefix() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] tsValue = timestampedValue(100L, "value".getBytes());
        final byte[] valueWithHeaders = headerFormatValue(tsValue);

        adapter.put(key, valueWithHeaders);

        verify(mockStore).put(eq(key), eq(tsValue));
    }

    @Test
    public void shouldHandlePutWithNull() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());

        adapter.put(key, null);

        verify(mockStore).put(eq(key), (byte[]) eq(null));
    }

    @Test
    public void shouldGetAndConvertToHeaderFormat() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] tsValue = timestampedValue(100L, "value".getBytes());
        when(mockStore.get(key)).thenReturn(tsValue);

        final byte[] result = adapter.get(key);

        assertArrayEquals(headerFormatValue(tsValue), result);
    }

    @Test
    public void shouldReturnNullWhenStoreReturnsNull() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        when(mockStore.get(key)).thenReturn(null);

        assertNull(adapter.get(key));
    }

    @Test
    public void shouldPutIfAbsentAndConvertResult() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] tsValue = timestampedValue(100L, "value".getBytes());
        final byte[] valueWithHeaders = headerFormatValue(tsValue);
        final byte[] oldTsValue = timestampedValue(50L, "old".getBytes());
        when(mockStore.putIfAbsent(eq(key), eq(tsValue))).thenReturn(oldTsValue);

        final byte[] result = adapter.putIfAbsent(key, valueWithHeaders);

        assertArrayEquals(headerFormatValue(oldTsValue), result);
    }

    @Test
    public void shouldDeleteAndConvertResult() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] oldTsValue = timestampedValue(50L, "old".getBytes());
        when(mockStore.delete(key)).thenReturn(oldTsValue);

        final byte[] result = adapter.delete(key);

        assertArrayEquals(headerFormatValue(oldTsValue), result);
    }

    @Test
    public void shouldPutAllEntries() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key1 = new Bytes("key1".getBytes());
        final Bytes key2 = new Bytes("key2".getBytes());
        final byte[] tsValue1 = timestampedValue(100L, "value1".getBytes());
        final byte[] tsValue2 = timestampedValue(200L, "value2".getBytes());

        final List<KeyValue<Bytes, byte[]>> entries = Arrays.asList(
            KeyValue.pair(key1, headerFormatValue(tsValue1)),
            KeyValue.pair(key2, headerFormatValue(tsValue2))
        );

        adapter.putAll(entries);

        verify(mockStore).put(eq(key1), eq(tsValue1));
        verify(mockStore).put(eq(key2), eq(tsValue2));
    }

    @Test
    public void shouldWrapRangeIterator() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.range(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.range(from, to);

        assertNotNull(result);
        assertTrue(result instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapReverseRangeIterator() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.reverseRange(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseRange(from, to);

        assertNotNull(result);
        assertTrue(result instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapAllIterator() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        when(mockStore.all()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.all();

        assertNotNull(result);
        assertTrue(result instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapReverseAllIterator() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        when(mockStore.reverseAll()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseAll();

        assertNotNull(result);
        assertTrue(result instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapPrefixScanIterator() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        when(mockStore.prefixScan(any(), any())).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.prefixScan("prefix", (topic, data) -> data.getBytes());

        assertNotNull(result);
        assertTrue(result instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldHandleKeyQuery() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] tsValue = timestampedValue(100L, "test-value".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(tsValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertArrayEquals(headerFormatValue(tsValue), result.getResult());
    }

    @Test
    public void shouldHandleKeyQueryWithNullResult() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
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
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
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
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
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
        assertTrue(result.getResult() instanceof TimestampedToHeadersIteratorAdapter);
    }

    @Test
    public void shouldCollectExecutionInfoForKeyQuery() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] tsValue = timestampedValue(100L, "test-value".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(tsValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(true));

        assertTrue(result.isSuccess());
        assertFalse(result.getExecutionInfo().isEmpty());
        final String executionInfo = String.join("\n", result.getExecutionInfo());
        assertTrue(executionInfo.contains("Handled in"));
        assertTrue(executionInfo.contains(TimestampedToHeadersStoreAdapter.class.getName()));
    }

    @Test
    public void shouldDelegateName() {
        when(mockStore.name()).thenReturn("test-store");
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();

        assertEquals("test-store", adapter.name());
    }

    @Test
    public void shouldReturnTrueForPersistent() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();

        assertTrue(adapter.persistent());
    }

    @Test
    public void shouldDelegateIsOpen() {
        when(mockStore.isOpen()).thenReturn(true);
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();

        assertTrue(adapter.isOpen());
    }

    @Test
    public void shouldDelegateApproximateNumEntries() {
        when(mockStore.approximateNumEntries()).thenReturn(42L);
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();

        assertEquals(42L, adapter.approximateNumEntries());
    }

    @Test
    public void shouldDelegateGetPosition() {
        final TimestampedToHeadersStoreAdapter adapter = createAdapter();
        when(mockStore.getPosition()).thenReturn(null);

        adapter.getPosition();

        verify(mockStore).getPosition();
    }

    /** Combined mock interface for a store that is both persistent and timestamped. */
    interface TimestampedAndPersistentStore extends KeyValueStore<Bytes, byte[]>, TimestampedBytesStore {
    }
}
