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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedToHeadersStoreAdapterTest {

    @Mock
    private KeyValueIterator<Bytes, byte[]> mockIterator;

    @SuppressWarnings("unchecked")
    private KeyValueStore<Bytes, byte[]> timestampedMockStore;

    private TimestampedToHeadersStoreAdapter adapter;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        timestampedMockStore = mock(KeyValueStore.class, withSettings().extraInterfaces(TimestampedBytesStore.class));
        when(timestampedMockStore.persistent()).thenReturn(true);
        adapter = new TimestampedToHeadersStoreAdapter(timestampedMockStore);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowIfStoreIsNotPersistent() {
        final KeyValueStore<Bytes, byte[]> nonPersistentStore =
            mock(KeyValueStore.class, withSettings().extraInterfaces(TimestampedBytesStore.class));
        when(nonPersistentStore.persistent()).thenReturn(false);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(nonPersistentStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a persistent store"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowIfStoreIsNotTimestamped() {
        final KeyValueStore<Bytes, byte[]> nonTimestampedStore = mock(KeyValueStore.class);
        when(nonTimestampedStore.persistent()).thenReturn(true);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new TimestampedToHeadersStoreAdapter(nonTimestampedStore)
        );

        assertTrue(exception.getMessage().contains("Provided store must be a timestamped store"));
    }

    @Test
    public void shouldPutRawTimestampedValueToStore() {
        final Bytes key = new Bytes("key".getBytes());
        final byte[] rawTimestampedValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
        final byte[] valueWithHeaders = convertToHeaderFormat(rawTimestampedValue);

        adapter.put(key, valueWithHeaders);

        verify(timestampedMockStore).put(eq(key), eq(rawTimestampedValue));
    }

    @Test
    public void shouldGetAndConvertToHeaderFormat() {
        final Bytes key = new Bytes("key".getBytes());
        final byte[] rawTimestampedValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
        when(timestampedMockStore.get(key)).thenReturn(rawTimestampedValue);

        final byte[] result = adapter.get(key);

        assertArrayEquals(convertToHeaderFormat(rawTimestampedValue), result);
    }

    @Test
    public void shouldReturnNullWhenStoreReturnsNull() {
        final Bytes key = new Bytes("key".getBytes());
        when(timestampedMockStore.get(key)).thenReturn(null);

        assertNull(adapter.get(key));
    }

    @Test
    public void shouldPutIfAbsentAndConvertResult() {
        final Bytes key = new Bytes("key".getBytes());
        final byte[] rawTimestampedValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
        final byte[] valueWithHeaders = convertToHeaderFormat(rawTimestampedValue);
        final byte[] oldRawValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 10, 'o', 'l', 'd'};
        when(timestampedMockStore.putIfAbsent(eq(key), eq(rawTimestampedValue))).thenReturn(oldRawValue);

        final byte[] result = adapter.putIfAbsent(key, valueWithHeaders);

        assertArrayEquals(convertToHeaderFormat(oldRawValue), result);
    }

    @Test
    public void shouldDeleteAndConvertResult() {
        final Bytes key = new Bytes("key".getBytes());
        final byte[] oldRawValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 10, 'o', 'l', 'd'};
        when(timestampedMockStore.delete(key)).thenReturn(oldRawValue);

        final byte[] result = adapter.delete(key);

        assertArrayEquals(convertToHeaderFormat(oldRawValue), result);
    }

    @Test
    public void shouldPutAllEntries() {
        final Bytes key1 = new Bytes("key1".getBytes());
        final Bytes key2 = new Bytes("key2".getBytes());
        final byte[] rawValue1 =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 'v', '1'};
        final byte[] rawValue2 =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 2, 'v', '2'};
        final byte[] value1 = convertToHeaderFormat(rawValue1);
        final byte[] value2 = convertToHeaderFormat(rawValue2);

        adapter.putAll(Arrays.asList(
            KeyValue.pair(key1, value1),
            KeyValue.pair(key2, value2)
        ));

        verify(timestampedMockStore).put(eq(key1), eq(rawValue1));
        verify(timestampedMockStore).put(eq(key2), eq(rawValue2));
    }

    @Test
    public void shouldWrapRangeIterator() {
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(timestampedMockStore.range(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.range(from, to);

        assertNotNull(result);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldWrapReverseRangeIterator() {
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(timestampedMockStore.reverseRange(from, to)).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseRange(from, to);

        assertNotNull(result);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldWrapAllIterator() {
        when(timestampedMockStore.all()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.all();

        assertNotNull(result);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldWrapReverseAllIterator() {
        when(timestampedMockStore.reverseAll()).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result = adapter.reverseAll();

        assertNotNull(result);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldWrapPrefixScanIterator() {
        when(timestampedMockStore.prefixScan(any(), any())).thenReturn(mockIterator);

        final KeyValueIterator<Bytes, byte[]> result =
            adapter.prefixScan("prefix", (topic, data) -> data.getBytes());

        assertNotNull(result);
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldHandleKeyQuery() {
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] rawTimestampedValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(rawTimestampedValue);
        when(timestampedMockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result =
            adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertArrayEquals(convertToHeaderFormat(rawTimestampedValue), result.getResult());
    }

    @Test
    public void shouldHandleKeyQueryWithNullResult() {
        final Bytes key = new Bytes("test-key".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(null);
        when(timestampedMockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result =
            adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertTrue(result.isSuccess());
        assertNull(result.getResult());
    }

    @Test
    public void shouldHandleFailedKeyQuery() {
        final Bytes key = new Bytes("test-key".getBytes());
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forUnknownQueryType(query, timestampedMockStore);
        when(timestampedMockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result =
            adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));

        assertFalse(result.isSuccess());
    }

    @Test
    public void shouldHandleRangeQuery() {
        final RangeQuery<Bytes, byte[]> query = RangeQuery.withRange(
            new Bytes("a".getBytes()),
            new Bytes("z".getBytes())
        );

        final QueryResult<KeyValueIterator<Bytes, byte[]>> mockResult =
            QueryResult.forResult(mockIterator);
        when(timestampedMockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<KeyValueIterator<Bytes, byte[]>> result = adapter.query(
            query,
            PositionBound.unbounded(),
            new QueryConfig(false)
        );

        assertTrue(result.isSuccess());
        assertNotNull(result.getResult());
        assertInstanceOf(TimestampedToHeadersIteratorAdapter.class, result.getResult());
    }

    @Test
    public void shouldCollectExecutionInfoForKeyQuery() {
        final Bytes key = new Bytes("test-key".getBytes());
        final byte[] rawTimestampedValue =
            new byte[] {0, 0, 0, 0, 0, 0, 0, 42, 'v', 'a', 'l'};
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(key);

        final QueryResult<byte[]> mockResult = QueryResult.forResult(rawTimestampedValue);
        when(timestampedMockStore.query(eq(query), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(mockResult);

        final QueryResult<byte[]> result =
            adapter.query(query, PositionBound.unbounded(), new QueryConfig(true));

        assertTrue(result.isSuccess());
        assertFalse(result.getExecutionInfo().isEmpty(),
            "Expected execution info to be collected");
        final String executionInfo = String.join("\n", result.getExecutionInfo());
        assertTrue(executionInfo.contains("Handled in"));
        assertTrue(executionInfo.contains(TimestampedToHeadersStoreAdapter.class.getName()));
    }

    @Test
    public void shouldDelegateName() {
        when(timestampedMockStore.name()).thenReturn("test-store");

        assertEquals("test-store", adapter.name());
    }

    @Test
    public void shouldReturnTrueForPersistent() {
        assertTrue(adapter.persistent());
    }

    @Test
    public void shouldDelegateIsOpen() {
        when(timestampedMockStore.isOpen()).thenReturn(true);

        assertTrue(adapter.isOpen());
    }

    @Test
    public void shouldDelegateApproximateNumEntries() {
        when(timestampedMockStore.approximateNumEntries()).thenReturn(42L);

        assertEquals(42L, adapter.approximateNumEntries());
    }

    @Test
    public void shouldDelegateGetPosition() {
        when(timestampedMockStore.getPosition()).thenReturn(null);

        adapter.getPosition();

        verify(timestampedMockStore).getPosition();
    }
}
