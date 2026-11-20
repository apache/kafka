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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Method-level coverage of {@link KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter} over a mocked
 * inner store: writes drop headers, reads (including iterators) convert {@link ValueAndTimestamp} into
 * {@link ValueTimestampHeaders} with empty headers, and the remaining {@link StateStore} methods delegate.
 * Strict stubs make a call to the wrong inner method fail the test instead of returning {@code null}. See
 * {@link KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapterCompletenessTest} for the
 * completeness guard (KAFKA-20328).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapterTest {

    private static final String KEY = "key";
    private static final String KEY_TO = "key-to";
    private static final long TIMESTAMP = 42L;
    private static final ValueAndTimestamp<String> INNER_VALUE = ValueAndTimestamp.make("value", TIMESTAMP);
    private static final ValueTimestampHeaders<String> EXPECTED_VALUE =
        ValueTimestampHeaders.make("value", TIMESTAMP, new RecordHeaders());

    @Mock
    private TimestampedKeyValueStore<String, String> inner;

    private KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter<String, String> adapter;

    @BeforeEach
    public void setUp() {
        adapter = new KeyValueTimestampedHeaderStoreToKeyValueTimestampStoreAdapter<>(inner);
    }

    private static ValueTimestampHeaders<String> withHeaders(final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("h", "v".getBytes());
        return ValueTimestampHeaders.make(value, TIMESTAMP, headers);
    }

    @Test
    public void shouldStripHeadersOnPut() {
        adapter.put(KEY, withHeaders("value"));
        verify(inner).put(KEY, INNER_VALUE);
    }

    @Test
    public void shouldForwardNullOnPut() {
        adapter.put(KEY, null);
        verify(inner).put(KEY, null);
    }

    @Test
    public void shouldStripHeadersOnPutAll() {
        adapter.putAll(List.of(KeyValue.pair(KEY, withHeaders("value")), KeyValue.pair(KEY_TO, withHeaders("other"))));
        verify(inner).putAll(List.of(
            KeyValue.pair(KEY, INNER_VALUE),
            KeyValue.pair(KEY_TO, ValueAndTimestamp.make("other", TIMESTAMP))));
    }

    @Test
    public void shouldStripHeadersOnPutIfAbsentAndConvertOldValue() {
        when(inner.putIfAbsent(KEY, INNER_VALUE)).thenReturn(ValueAndTimestamp.make("old", 7L));
        assertEquals(ValueTimestampHeaders.make("old", 7L, new RecordHeaders()), adapter.putIfAbsent(KEY, withHeaders("value")));
    }

    @Test
    public void shouldReturnNullFromPutIfAbsentWhenAbsent() {
        when(inner.putIfAbsent(KEY, null)).thenReturn(null);
        assertNull(adapter.putIfAbsent(KEY, null));
    }

    @Test
    public void shouldConvertGet() {
        when(inner.get(KEY)).thenReturn(INNER_VALUE);
        assertEquals(EXPECTED_VALUE, adapter.get(KEY));
    }

    @Test
    public void shouldReturnNullFromGetWhenMissing() {
        when(inner.get(KEY)).thenReturn(null);
        assertNull(adapter.get(KEY));
    }

    @Test
    public void shouldConvertDelete() {
        when(inner.delete(KEY)).thenReturn(INNER_VALUE);
        assertEquals(EXPECTED_VALUE, adapter.delete(KEY));
    }

    @Test
    public void shouldReturnNullFromDeleteWhenMissing() {
        when(inner.delete(KEY)).thenReturn(null);
        assertNull(adapter.delete(KEY));
    }

    // Each call is applied to the inner store to stub it and to the adapter to exercise it, so the
    // adapter must forward to the same method with the same arguments.
    private static Stream<Arguments> iteratorCalls() {
        final StringSerializer prefixSerializer = new StringSerializer();
        return Stream.of(
            call("range", s -> s.range(KEY, KEY_TO)),
            call("reverseRange", s -> s.reverseRange(KEY, KEY_TO)),
            call("all", KeyValueStore::all),
            call("reverseAll", KeyValueStore::reverseAll),
            call("prefixScan", s -> s.prefixScan("k", prefixSerializer))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("iteratorCalls")
    public void shouldConvertIteratorValuesAndDelegateIteratorMethods(
        final Function<KeyValueStore<String, ?>, KeyValueIterator<String, ?>> call) {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<String, ValueAndTimestamp<String>> innerIterator = mock(KeyValueIterator.class);
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.peekNextKey()).thenReturn(KEY);
        when(innerIterator.next()).thenReturn(KeyValue.pair(KEY, INNER_VALUE));
        when(call.apply(inner)).thenAnswer(invocation -> innerIterator);

        final KeyValueIterator<String, ?> iterator = call.apply(adapter);

        assertTrue(iterator.hasNext());
        assertEquals(KEY, iterator.peekNextKey());
        assertEquals(KeyValue.pair(KEY, EXPECTED_VALUE), iterator.next());
        iterator.close();
        verify(innerIterator).close();
    }

    @Test
    public void shouldDelegateApproximateNumEntries() {
        when(inner.approximateNumEntries()).thenReturn(5L);
        assertEquals(5L, adapter.approximateNumEntries());
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
    public void shouldDelegatePersistent() {
        when(inner.persistent()).thenReturn(false);
        assertFalse(adapter.persistent());
    }

    @Test
    public void shouldDelegateInit() {
        final StateStoreContext context = mock(StateStoreContext.class);
        final StateStore root = mock(StateStore.class);
        adapter.init(context, root);
        verify(inner).init(context, root);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateFlush() {
        adapter.flush();
        verify(inner).flush();
    }

    @Test
    public void shouldDelegateCommit() {
        final Map<TopicPartition, Long> changelogOffsets = Map.of(new TopicPartition("changelog", 0), 7L);
        adapter.commit(changelogOffsets);
        verify(inner).commit(changelogOffsets);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateManagesOffsets() {
        when(inner.managesOffsets()).thenReturn(true);
        assertTrue(adapter.managesOffsets());
    }

    @Test
    public void shouldDelegateCommittedOffset() {
        final TopicPartition partition = new TopicPartition("changelog", 0);
        when(inner.committedOffset(partition)).thenReturn(7L);
        assertEquals(7L, adapter.committedOffset(partition));
    }

    @Test
    public void shouldDelegateApproximateNumUncommittedBytes() {
        when(inner.approximateNumUncommittedBytes()).thenReturn(123L);
        assertEquals(123L, adapter.approximateNumUncommittedBytes());
    }

    @Test
    public void shouldDelegateQuery() {
        final Query<ValueAndTimestamp<String>> query = KeyQuery.withKey(KEY);
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);
        final QueryResult<ValueAndTimestamp<String>> result = QueryResult.forResult(INNER_VALUE);
        when(inner.query(query, positionBound, config)).thenReturn(result);
        assertSame(result, adapter.query(query, positionBound, config));
    }

    @Test
    public void shouldDelegateGetPosition() {
        final Position position = Position.emptyPosition();
        when(inner.getPosition()).thenReturn(position);
        assertSame(position, adapter.getPosition());
    }

    @Test
    public void shouldDelegateClose() {
        adapter.close();
        verify(inner).close();
    }

    private static Arguments call(final String name,
                                  final Function<KeyValueStore<String, ?>, KeyValueIterator<String, ?>> call) {
        return Arguments.of(Named.of(name, call));
    }
}
