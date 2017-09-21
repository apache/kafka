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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.test.ReadOnlySessionStoreStub;
import org.apache.kafka.test.StateStoreProviderStub;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class CompositeReadOnlySessionStoreTest {

    private final String storeName = "session-store";
    private final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
    private final StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);
    private final ReadOnlySessionStoreStub<String, Long> underlyingSessionStore = new ReadOnlySessionStoreStub<>();
    private final ReadOnlySessionStoreStub<String, Long> otherUnderlyingStore = new ReadOnlySessionStoreStub<>();
    private CompositeReadOnlySessionStore<String, Long> sessionStore;

    @Before
    public void before() {
        stubProviderOne.addStore(storeName, underlyingSessionStore);
        stubProviderOne.addStore("other-session-store", otherUnderlyingStore);


        sessionStore = new CompositeReadOnlySessionStore<>(
                new WrappingStoreProvider(Arrays.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                QueryableStoreTypes.<String, Long>sessionStore(), storeName);
    }

    @Test
    public void shouldFetchResulstFromUnderlyingSessionStore() {
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(10, 10)), 2L);

        final List<KeyValue<Windowed<String>, Long>> results = toList(sessionStore.fetch("a"));
        assertEquals(Arrays.asList(KeyValue.pair(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                   KeyValue.pair(new Windowed<>("a", new SessionWindow(10, 10)), 2L)),
                     results);
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoData() {
        final KeyValueIterator<Windowed<String>, Long> result = sessionStore.fetch("b");
        assertFalse(result.hasNext());
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() {
        final ReadOnlySessionStoreStub<String, Long> secondUnderlying = new
                ReadOnlySessionStoreStub<>();
        stubProviderTwo.addStore(storeName, secondUnderlying);

        final Windowed<String> keyOne = new Windowed<>("key-one", new SessionWindow(0, 0));
        final Windowed<String> keyTwo = new Windowed<>("key-two", new SessionWindow(0, 0));
        underlyingSessionStore.put(keyOne, 0L);
        secondUnderlying.put(keyTwo, 10L);

        final List<KeyValue<Windowed<String>, Long>> keyOneResults = toList(sessionStore.fetch("key-one"));
        final List<KeyValue<Windowed<String>, Long>> keyTwoResults = toList(sessionStore.fetch("key-two"));

        assertEquals(Collections.singletonList(KeyValue.pair(keyOne, 0L)), keyOneResults);
        assertEquals(Collections.singletonList(KeyValue.pair(keyTwo, 10L)), keyTwoResults);
    }

    @Test
    public void shouldNotGetValueFromOtherStores() {
        final Windowed<String> expectedKey = new Windowed<>("foo", new SessionWindow(0, 0));
        otherUnderlyingStore.put(new Windowed<>("foo", new SessionWindow(10, 10)), 10L);
        underlyingSessionStore.put(expectedKey, 1L);

        final KeyValueIterator<Windowed<String>, Long> result = sessionStore.fetch("foo");
        assertEquals(KeyValue.pair(expectedKey, 1L), result.next());
        assertFalse(result.hasNext());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStateStoreExceptionOnRebalance() {
        final CompositeReadOnlySessionStore<String, String> store
                = new CompositeReadOnlySessionStore<>(new StateStoreProviderStub(true),
                                                      QueryableStoreTypes.<String, String>sessionStore(),
                                                      "whateva");

        store.fetch("a");
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionIfSessionFetchThrows() {
        underlyingSessionStore.setOpen(false);
        try {
            sessionStore.fetch("key");
            fail("Should have thrown InvalidStateStoreException with session store");
        } catch (InvalidStateStoreException e) { }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionIfFetchingNullKey() {
        sessionStore.fetch(null);
    }

    @Test
    public void shouldFetchKeyRangeAcrossStores() {
        final ReadOnlySessionStoreStub<String, Long> secondUnderlying = new
                ReadOnlySessionStoreStub<>();
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 0L);
        secondUnderlying.put(new Windowed<>("b", new SessionWindow(0, 0)), 10L);
        final List<KeyValue<Windowed<String>, Long>> results = StreamsTestUtils.toList(sessionStore.fetch("a", "b"));
        assertThat(results.size(), equalTo(2));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEIfKeyIsNull() {
        underlyingSessionStore.fetch(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEIfFromKeyIsNull() {
        underlyingSessionStore.fetch(null, "a");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEIfToKeyIsNull() {
        underlyingSessionStore.fetch("a", null);
    }
}