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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.StateStoreProviderStub;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompositeReadOnlyKeyValueStoreTest {

    private final String storeName = "my-store";
    private final String storeNameA = "my-storeA";
    private StateStoreProviderStub stubProviderTwo;
    private KeyValueStore<String, String> stubOneUnderlying;
    private KeyValueStore<String, String> otherUnderlyingStore;
    private CompositeReadOnlyKeyValueStore<String, String> theStore;
    private CompositeReadOnlyKeyValueStore<String, String> rebalancingStore;
    private CompositeReadOnlyKeyValueStore<String, String> notAvailableStore;

    @Before
    public void before() {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = newStoreInstance();
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = newStoreInstance();
        stubProviderOne.addStore("other-store", otherUnderlyingStore);

        theStore = createKeyValueStore(State.RUNNING, storeName,
                new WrappingStoreProvider(Arrays.asList(stubProviderOne, stubProviderTwo))
            );
        rebalancingStore = createKeyValueStore(State.REBALANCING, storeName, new StateStoreProviderStub(true));
        notAvailableStore = createKeyValueStore(State.PENDING_SHUTDOWN, storeName, new StateStoreProviderStub(true));
    }

    private KeyValueStore<String, String> newStoreInstance() {
        final KeyValueStore<String, String> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                Serdes.String())
                .build();

        store.init(new InternalMockProcessorContext(new StateSerdes<>(ProcessorStateManager.storeChangelogTopic("appId", storeName), Serdes.String(), Serdes.String()),
                                                    new NoOpRecordCollector()),
                store);

        return store;
    }

    @Test
    public void shouldReturnNullIfKeyDoesntExist() {
        assertNull(theStore.get("whatever"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        theStore.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        theStore.range(null, "to");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        theStore.range("from", null);
    }

    @Test
    public void shouldReturnValueIfExists() {
        stubOneUnderlying.put("key", "value");
        assertEquals("value", theStore.get("key"));
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put("otherKey", "otherValue");
        assertNull(theStore.get("otherKey"));
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWhileNext() {
        stubOneUnderlying.put("a", "1");
        final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.next();
            fail("Should have thrown NoSuchElementException with next()");
        } catch (final NoSuchElementException e) { }
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWhilePeekNext() {
        stubOneUnderlying.put("a", "1");
        final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.peekNextKey();
            fail("Should have thrown NoSuchElementException with peekNextKey()");
        } catch (final NoSuchElementException e) { }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhileRemove() {
        final KeyValueIterator<String, String> keyValueIterator = theStore.all();
        try {
            keyValueIterator.remove();
            fail("Should have thrown UnsupportedOperationException");
        } catch (final UnsupportedOperationException e) { }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhileRange() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b");
        try {
            keyValueIterator.remove();
            fail("Should have thrown UnsupportedOperationException");
        } catch (final UnsupportedOperationException e) { }
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        cache.put("key-two", "key-two-value");
        stubOneUnderlying.put("key-one", "key-one-value");

        assertEquals("key-two-value", theStore.get("key-two"));
        assertEquals("key-one-value", theStore.get("key-one"));
    }

    @Test
    public void shouldSupportRange() {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("c", "c");

        final List<KeyValue<String, String>> results = toList(theStore.range("a", "b"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertEquals(2, results.size());
    }

    @Test
    public void shouldSupportRangeAcrossMultipleKVStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.range("a", "e"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertEquals(4, results.size());
    }

    @Test
    public void shouldSupportAllAcrossMultipleStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.all());
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertTrue(results.contains(new KeyValue<>("x", "x")));
        assertTrue(results.contains(new KeyValue<>("z", "z")));
        assertEquals(6, results.size());
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionDuringRebalance() {
        rebalancingStore.get("anything");
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionDuringStoreNotAvailable() {
        notAvailableStore.get("anything");
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionOnApproximateNumEntriesDuringRebalance() {
        rebalancingStore.approximateNumEntries();
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionOnApproximateNumEntriesDuringStoreNotAvailable() {
        notAvailableStore.approximateNumEntries();
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionOnRangeDuringRebalance() {
        rebalancingStore.range("anything", "something");
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionOnRangeDuringStoreNotAvailable() {
        notAvailableStore.range("anything", "something");
    }

    @Test(expected = StateStoreMigratedException.class)
    public void shouldThrowStateStoreMigratedExceptionOnAllDuringRebalance() {
        rebalancingStore.all();
    }

    @Test(expected = StateStoreNotAvailableException.class)
    public void shouldThrowStateStoreNotAvailableExceptionOnAllDuringStoreNotAvailable() {
        notAvailableStore.all();
    }

    @Test
    public void shouldGetApproximateEntriesAcrossAllStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        assertEquals(6, theStore.approximateNumEntries());
    }

    @Test
    public void shouldReturnLongMaxValueOnOverflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        stubOneUnderlying.put("overflow", "me");
        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }

    @Test
    public void shouldReturnLongMaxValueOnUnderflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });
        stubProviderTwo.addStore(storeNameA, new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }

    private CompositeReadOnlyKeyValueStore<String, String> createKeyValueStore(final State state, final String storeName,
                                                                               final StateStoreProvider storeProvider) {
        final KafkaStreams kafkaStreams = StreamsTestUtils.mockStreams(state);
        return new CompositeReadOnlyKeyValueStore<>(kafkaStreams,
                storeProvider, QueryableStoreTypes.keyValueStore(), storeName);
    }

}