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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Method-level coverage of {@link SessionHeadersStoreToSessionStoreAdapter} over a mocked inner store: put drops
 * headers, reads (including all iterator-returning methods) wrap the plain aggregate in
 * {@link AggregationWithHeaders} with empty headers, and writes/removes delegate. Strict stubs make a call to the
 * wrong inner overload (e.g. a {@code backwardFindSessions} forwarded to {@code findSessions}) fail the test
 * instead of returning {@code null}. The remaining {@code StateStore} methods are inherited from
 * {@code WrappedStateStore}. See {@code StoreAdapterCompletenessTest} for the completeness guard (KAFKA-20328).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionHeadersStoreToSessionStoreAdapterTest {

    private static final String KEY = "key";
    private static final String KEY_TO = "key-to";
    private static final Windowed<String> SESSION_KEY = new Windowed<>(KEY, new SessionWindow(0L, 10L));
    private static final Instant T_FROM = Instant.ofEpochMilli(0L);
    private static final Instant T_TO = Instant.ofEpochMilli(10L);
    private static final AggregationWithHeaders<String> EXPECTED_AGGREGATE =
        AggregationWithHeaders.make("agg", new RecordHeaders());

    @Mock
    private SessionStore<String, String> inner;

    private SessionHeadersStoreToSessionStoreAdapter<String, String> adapter;

    @BeforeEach
    public void setUp() {
        adapter = new SessionHeadersStoreToSessionStoreAdapter<>(inner);
    }

    @Test
    public void shouldRejectHeadersStoreAsInner() {
        @SuppressWarnings("unchecked")
        final SessionStoreWithHeaders<String, String> headersStore = mock(SessionStoreWithHeaders.class);
        assertThrows(ClassCastException.class, () -> new SessionHeadersStoreToSessionStoreAdapter<>(headersStore));
    }

    @Test
    public void shouldStripHeadersOnPut() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("h", "v".getBytes());
        adapter.put(SESSION_KEY, AggregationWithHeaders.make("agg", headers));
        verify(inner).put(SESSION_KEY, "agg");
    }

    @Test
    public void shouldForwardNullOnPut() {
        adapter.put(SESSION_KEY, null);
        verify(inner).put(SESSION_KEY, null);
    }

    @Test
    public void shouldDelegateRemove() {
        adapter.remove(SESSION_KEY);
        verify(inner).remove(SESSION_KEY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateFlush() {
        adapter.flush();
        verify(inner).flush();
    }

    private static Stream<Arguments> fetchSessionCalls() {
        return Stream.of(
            call("fetchSession(key, long, long)", s -> s.fetchSession(KEY, 0L, 10L)),
            call("fetchSession(key, Instant, Instant)", s -> s.fetchSession(KEY, T_FROM, T_TO))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fetchSessionCalls")
    public void shouldWrapFetchedSessionWithEmptyHeaders(final Function<SessionStore<String, ?>, Object> call) {
        when(call.apply(inner)).thenAnswer(invocation -> "agg");
        assertEquals(EXPECTED_AGGREGATE, call.apply(adapter));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fetchSessionCalls")
    public void shouldReturnNullWhenFetchedSessionIsMissing(final Function<SessionStore<String, ?>, Object> call) {
        when(call.apply(inner)).thenAnswer(invocation -> null);
        assertNull(call.apply(adapter));
    }

    // Each call is applied to the inner store to stub it and to the adapter to exercise it, so the
    // adapter must forward to the same overload with the same arguments.
    private static Stream<Arguments> iteratorCalls() {
        return Stream.of(
            call("findSessions(long, long)", s -> s.findSessions(0L, 10L)),
            call("findSessions(key, long, long)", s -> s.findSessions(KEY, 0L, 10L)),
            call("findSessions(key, Instant, Instant)", s -> s.findSessions(KEY, T_FROM, T_TO)),
            call("findSessions(keyFrom, keyTo, long, long)", s -> s.findSessions(KEY, KEY_TO, 0L, 10L)),
            call("findSessions(keyFrom, keyTo, Instant, Instant)", s -> s.findSessions(KEY, KEY_TO, T_FROM, T_TO)),
            call("backwardFindSessions(key, long, long)", s -> s.backwardFindSessions(KEY, 0L, 10L)),
            call("backwardFindSessions(key, Instant, Instant)", s -> s.backwardFindSessions(KEY, T_FROM, T_TO)),
            call("backwardFindSessions(keyFrom, keyTo, long, long)", s -> s.backwardFindSessions(KEY, KEY_TO, 0L, 10L)),
            call("backwardFindSessions(keyFrom, keyTo, Instant, Instant)", s -> s.backwardFindSessions(KEY, KEY_TO, T_FROM, T_TO)),
            call("fetch(key)", s -> s.fetch(KEY)),
            call("fetch(keyFrom, keyTo)", s -> s.fetch(KEY, KEY_TO)),
            call("backwardFetch(key)", s -> s.backwardFetch(KEY)),
            call("backwardFetch(keyFrom, keyTo)", s -> s.backwardFetch(KEY, KEY_TO))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("iteratorCalls")
    public void shouldWrapIteratorValuesAndDelegateIteratorMethods(
        final Function<SessionStore<String, ?>, Object> call) {
        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, String> innerIterator = mock(KeyValueIterator.class);
        when(innerIterator.hasNext()).thenReturn(true);
        when(innerIterator.peekNextKey()).thenReturn(SESSION_KEY);
        when(innerIterator.next()).thenReturn(KeyValue.pair(SESSION_KEY, "agg"));
        when(call.apply(inner)).thenAnswer(invocation -> innerIterator);

        @SuppressWarnings("unchecked")
        final KeyValueIterator<Windowed<String>, ?> iterator = (KeyValueIterator<Windowed<String>, ?>) call.apply(adapter);

        assertTrue(iterator.hasNext());
        assertEquals(SESSION_KEY, iterator.peekNextKey());
        assertEquals(KeyValue.pair(SESSION_KEY, EXPECTED_AGGREGATE), iterator.next());
        iterator.close();
        verify(innerIterator).close();
    }

    private static Arguments call(final String name, final Function<SessionStore<String, ?>, Object> call) {
        return Arguments.of(Named.of(name, call));
    }
}
