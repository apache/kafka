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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionToHeadersStoreAdapterTest {

    private static final Bytes KEY = Bytes.wrap("key".getBytes());
    private static final Bytes KEY_FROM = Bytes.wrap("a".getBytes());
    private static final Bytes KEY_TO = Bytes.wrap("z".getBytes());
    private static final byte[] RAW_VALUE = "value".getBytes();
    private static final byte[] VALUE_WITH_EMPTY_HEADERS = convertToHeaderFormat(RAW_VALUE);
    private static final Windowed<Bytes> SESSION_KEY = new Windowed<>(KEY, new SessionWindow(10L, 20L));

    @Mock
    private SessionStore<Bytes, byte[]> innerStore;

    private SessionToHeadersStoreAdapter adapter;

    @BeforeEach
    public void setUp() {
        when(innerStore.persistent()).thenReturn(true);
        adapter = new SessionToHeadersStoreAdapter(innerStore);
    }

    @Test
    public void shouldThrowIfStoreIsNotPersistent() {
        final SessionStore<Bytes, byte[]> nonPersistentStore = mock(SessionStore.class);
        when(nonPersistentStore.persistent()).thenReturn(false);
        assertThrows(IllegalArgumentException.class,
            () -> new SessionToHeadersStoreAdapter(nonPersistentStore));
    }

    @Test
    public void shouldPutWithEmptyHeaders() {
        adapter.put(SESSION_KEY, VALUE_WITH_EMPTY_HEADERS);
        verify(innerStore).put(SESSION_KEY, RAW_VALUE);
    }

    @Test
    public void shouldPutNullValue() {
        adapter.put(SESSION_KEY, null);
        verify(innerStore).put(SESSION_KEY, null);
    }

    @Test
    public void shouldFetchSessionAndStripHeaders() {
        when(innerStore.fetchSession(KEY, 10L, 20L)).thenReturn(RAW_VALUE);
        final byte[] result = adapter.fetchSession(KEY, 10L, 20L);
        assertArrayEquals(VALUE_WITH_EMPTY_HEADERS, result);
    }

    @Test
    public void shouldReturnNullForNullFetchSession() {
        when(innerStore.fetchSession(KEY, 10L, 20L)).thenReturn(null);
        assertNull(adapter.fetchSession(KEY, 10L, 20L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapFindSessionsIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.findSessions(KEY, 10L, 20L)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.findSessions(KEY, 10L, 20L);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapBackwardFindSessionsIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.backwardFindSessions(KEY, 10L, 20L)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.backwardFindSessions(KEY, 10L, 20L);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapFindSessionsWithKeyRangeIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.findSessions(KEY_FROM, KEY_TO, 10L, 20L)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result =
            adapter.findSessions(KEY_FROM, KEY_TO, 10L, 20L);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapBackwardFindSessionsWithKeyRangeIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.backwardFindSessions(KEY_FROM, KEY_TO, 10L, 20L)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result =
            adapter.backwardFindSessions(KEY_FROM, KEY_TO, 10L, 20L);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapFetchIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.fetch(KEY)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.fetch(KEY);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapBackwardFetchIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.backwardFetch(KEY)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.backwardFetch(KEY);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapFetchWithKeyRangeIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.fetch(KEY_FROM, KEY_TO)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.fetch(KEY_FROM, KEY_TO);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldWrapBackwardFetchWithKeyRangeIterator() {
        final KeyValueIterator<Windowed<Bytes>, byte[]> innerIter = mock(KeyValueIterator.class);
        when(innerStore.backwardFetch(KEY_FROM, KEY_TO)).thenReturn(innerIter);
        final KeyValueIterator<Windowed<Bytes>, byte[]> result = adapter.backwardFetch(KEY_FROM, KEY_TO);
        assertInstanceOf(SessionToHeadersIteratorAdapter.class, result);
    }

    @Test
    public void shouldDelegateRemove() {
        adapter.remove(SESSION_KEY);
        verify(innerStore).remove(SESSION_KEY);
    }

    @Test
    public void shouldDelegateInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore root = mock(StateStore.class);
        adapter.init(context, root);
        verify(innerStore).init(context, root);
    }

    @Test
    public void shouldDelegateClose() {
        adapter.close();
        verify(innerStore).close();
    }

    @Test
    public void shouldReturnPersistentTrue() {
        assertTrue(adapter.persistent());
    }

    @Test
    public void shouldDelegateIsOpen() {
        when(innerStore.isOpen()).thenReturn(true);
        assertTrue(adapter.isOpen());
    }

    @Test
    public void shouldDelegateName() {
        when(innerStore.name()).thenReturn("test-store");
        assertEquals("test-store", adapter.name());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDelegateQueryToInnerStore() {
        final QueryResult<Void> expectedResult = QueryResult.forFailure(
            FailureReason.UNKNOWN_QUERY_TYPE, "unknown");
        when(innerStore.query(any(Query.class), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(expectedResult);

        final Query<Void> query = new Query<Void>() { };
        final QueryResult<Void> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(false));
        assertTrue(result.isFailure());
        assertEquals(FailureReason.UNKNOWN_QUERY_TYPE, result.getFailureReason());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAddExecutionInfoOnQuery() {
        final QueryResult<Void> expectedResult = QueryResult.forFailure(
            FailureReason.UNKNOWN_QUERY_TYPE, "unknown");
        when(innerStore.query(any(Query.class), any(PositionBound.class), any(QueryConfig.class)))
            .thenReturn(expectedResult);

        final Query<Void> query = new Query<Void>() { };
        final QueryResult<Void> result = adapter.query(query, PositionBound.unbounded(), new QueryConfig(true));
        assertTrue(result.getExecutionInfo().stream()
            .anyMatch(info -> info.contains("SessionToHeadersStoreAdapter")));
    }

    @Test
    public void shouldStripHeadersFromRawAggregationValue() {
        final byte[] result = AggregationWithHeadersDeserializer.rawAggregation(VALUE_WITH_EMPTY_HEADERS);
        assertArrayEquals(RAW_VALUE, result);
    }

    @Test
    public void shouldReturnNullFromRawAggregationValueForNull() {
        assertNull(AggregationWithHeadersDeserializer.rawAggregation(null));
    }
}
