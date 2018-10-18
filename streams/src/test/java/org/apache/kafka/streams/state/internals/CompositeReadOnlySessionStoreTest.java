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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
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

public class CompositeReadOnlySessionStoreTest {

    private final String storeName = "session-store";
    private final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
    private final StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);
    private final StateStoreProviderStub stubProviderThree = new StateStoreProviderStub(true);
    private final ReadOnlySessionStoreStub<String, Long> underlyingSessionStore = new ReadOnlySessionStoreStub<>();
    private final ReadOnlySessionStoreStub<String, Long> otherUnderlyingStore = new ReadOnlySessionStoreStub<>();
    private CompositeReadOnlySessionStore<String, Long> sessionStore;
    private CompositeReadOnlySessionStore<String, Long> rebalancingStore;
    private CompositeReadOnlySessionStore<String, Long> notAvailableStore;


    @Before
    public void before() {
        stubProviderOne.addStore(storeName, underlyingSessionStore);
        stubProviderOne.addStore("other-session-store", otherUnderlyingStore);

        sessionStore = createSessionStore(State.RUNNING, storeName,
                new WrappingStoreProvider(Arrays.asList(stubProviderOne, stubProviderTwo))
            );
        rebalancingStore = createSessionStore(State.REBALANCING, storeName, stubProviderThree);
        notAvailableStore = createSessionStore(State.PENDING_SHUTDOWN, storeName, stubProviderThree);
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

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionOnFetchByKeyDuringRebalance() {
        rebalancingStore.fetch("a");
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionOnFetchByKeyDuringStoreNotAvailable() {
        notAvailableStore.fetch("a");
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionOnFetchByRangeDuringRebalance() {
        rebalancingStore.fetch("a", "z");
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionOnFetchByRangeDuringStoreNotAvailable() {
        notAvailableStore.fetch("a", "z");
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionIfSessionFetchThrows() {
        underlyingSessionStore.setOpen(false);
        sessionStore.fetch("key");
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

    private CompositeReadOnlySessionStore<String, Long> createSessionStore(final State streamState, final String storeName,
                                                                             final StateStoreProvider storeProvider) {
        final KafkaStreams kafkaStreams = StreamsTestUtils.mockStreams(streamState);
        return new CompositeReadOnlySessionStore<>(kafkaStreams,
                storeProvider, QueryableStoreTypes.sessionStore(), storeName);
    }

}