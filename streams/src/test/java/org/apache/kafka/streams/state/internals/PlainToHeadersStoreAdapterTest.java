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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertFromPlainToHeaderFormat;
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
public class PlainToHeadersStoreAdapterTest {

    @Mock
    private KeyValueStore<Bytes, byte[]> mockStore;

    @Mock
    private KeyValueIterator<Bytes, byte[]> mockIterator;

    private PlainToHeadersStoreAdapter adapter;

    private PlainToHeadersStoreAdapter createAdapter() {
        when(mockStore.persistent()).thenReturn(true);
        return new PlainToHeadersStoreAdapter(mockStore);
    }

    @Test
    public void shouldThrowIfStoreIsNotPersistent() {
        when(mockStore.persistent()).thenReturn(false);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PlainToHeadersStoreAdapter(mockStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a persistent store"));
    }

    @Test
    public void shouldThrowIfStoreIsTimestamped() {
        final RocksDBTimestampedStore timestampedStore = new RocksDBTimestampedStore("test", "scope");

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PlainToHeadersStoreAdapter(timestampedStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a plain (non-timestamped)"));
    }

    @Test
    public void shouldPutRawPlainValueToStore() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] plainValue = "value".getBytes();
        final byte[] valueWithHeaders = convertFromPlainToHeaderFormat(plainValue);

        adapter.put(key, valueWithHeaders);

        verify(mockStore).put(eq(key), eq(plainValue));
    }

    @Test
    public void shouldGetAndConvertFromPlainToHeaderFormat() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] plainValue = "value".getBytes();
        when(mockStore.get(key)).thenReturn(plainValue);

        final byte[] result = adapter.get(key);

        assertArrayEquals(convertFromPlainToHeaderFormat(plainValue), result);
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
        final byte[] plainValue = "value".getBytes();
        final byte[] valueWithHeaders = convertFromPlainToHeaderFormat(plainValue);
        final byte[] oldPlainValue = "oldValue".getBytes();
        when(mockStore.putIfAbsent(eq(key), eq(plainValue))).thenReturn(oldPlainValue);

        final byte[] result = adapter.putIfAbsent(key, valueWithHeaders);

        assertArrayEquals(convertFromPlainToHeaderFormat(oldPlainValue), result);
    }

    @Test
    public void shouldDeleteAndConvertResult() {
        adapter = createAdapter();
        final Bytes key = new Bytes("key".getBytes());
        final byte[] oldPlainValue = "oldValue".getBytes();
        when(mockStore.delete(key)).thenReturn(oldPlainValue);

        final byte[] result = adapter.delete(key);

        assertArrayEquals(convertFromPlainToHeaderFormat(oldPlainValue), result);
    }

    @Test
    public void shouldPutAllEntries() {
        adapter = createAdapter();
        final Bytes key1 = new Bytes("key1".getBytes());
        final Bytes key2 = new Bytes("key2".getBytes());
        final byte[] value1 = convertFromPlainToHeaderFormat("value1".getBytes());
        final byte[] value2 = convertFromPlainToHeaderFormat("value2".getBytes());

        final List<KeyValue<Bytes, byte[]>> entries = Arrays.asList(
            KeyValue.pair(key1, value1),
            KeyValue.pair(key2, value2)
        );

        adapter.putAll(entries);

        verify(mockStore).put(eq(key1), eq("value1".getBytes()));
        verify(mockStore).put(eq(key2), eq("value2".getBytes()));
    }

    @Test
    public void shouldWrapRangeIterator() {
        adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.range(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.range(from, to);

        assertNotNull(result);
        assertTrue(result instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapReverseRangeIterator() {
        adapter = createAdapter();
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(mockStore.reverseRange(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseRange(from, to);

        assertNotNull(result);
        assertTrue(result instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapAllIterator() {
        adapter = createAdapter();
        when(mockStore.all()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.all();

        assertNotNull(result);
        assertTrue(result instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapReverseAllIterator() {
        adapter = createAdapter();
        when(mockStore.reverseAll()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseAll();

        assertNotNull(result);
        assertTrue(result instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldWrapPrefixScanIterator() {
        adapter = createAdapter();
        when(mockStore.prefixScan(any(), any())).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.prefixScan("prefix", (topic, data) -> data.getBytes());

        assertNotNull(result);
        assertTrue(result instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldHandleKeyQuery() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] plainValue = "test-value".getBytes();
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(plainValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertArrayEquals(convertFromPlainToHeaderFormat(plainValue), result.getResult());
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
        assertTrue(result.getResult() instanceof PlainToHeadersIteratorAdapter);
    }

    @Test
    public void shouldCollectExecutionInfoForKeyQuery() {
        adapter = createAdapter();
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] plainValue = "test-value".getBytes();
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(plainValue);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(true));

        assertTrue(result.isSuccess());
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        final String executionInfo = String.join("\n", result.getExecutionInfo());
        assertTrue(executionInfo.contains("Handled in"), "Expected execution info to contain handling information");
        assertTrue(executionInfo.contains(PlainToHeadersStoreAdapter.class.getName()),
            "Expected execution info to mention PlainToHeadersStoreAdapter");
    }

    @Test
    public void shouldCollectExecutionInfoForRangeQuery() {
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
            new QueryConfig(true)
        );

        assertTrue(result.isSuccess());
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        final String executionInfo = String.join("\n", result.getExecutionInfo());
        assertTrue(executionInfo.contains("Handled in"), "Expected execution info to contain handling information");
    }

    @Test
    public void shouldDelegateOtherQueryTypesToUnderlyingStore() {
        adapter = createAdapter();
        // Create a custom query type that's not KeyQuery or RangeQuery
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("key".getBytes()));

        final QueryResult<byte[]> mockResult = QueryResult.forUnknownQueryType(query, mockStore);
        when(mockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        // The result should be passed through from the underlying store
        assertFalse(result.isSuccess());
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