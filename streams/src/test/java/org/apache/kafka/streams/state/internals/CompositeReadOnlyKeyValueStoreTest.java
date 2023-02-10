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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.StateStoreProviderStub;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CompositeReadOnlyKeyValueStoreTest {

    private final String storeName = "my-store";
    private StateStoreProviderStub stubProviderTwo;
    private KeyValueStore<String, String> stubOneUnderlying;
    private KeyValueStore<String, String> otherUnderlyingStore;
    private CompositeReadOnlyKeyValueStore<String, String> theStore;

    @Before
    public void before() {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = newStoreInstance();
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = newStoreInstance();
        stubProviderOne.addStore("other-store", otherUnderlyingStore);
        theStore = new CompositeReadOnlyKeyValueStore<>(
            new WrappingStoreProvider(asList(stubProviderOne, stubProviderTwo), StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())),
            QueryableStoreTypes.keyValueStore(),
            storeName
        );
    }

    private KeyValueStore<String, String> newStoreInstance() {
        final KeyValueStore<String, String> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.String())
            .build();

        @SuppressWarnings("rawtypes") final InternalMockProcessorContext context =
            new InternalMockProcessorContext<>(
                new StateSerdes<>(
                    ProcessorStateManager.storeChangelogTopic("appId", storeName, null),
                    Serdes.String(),
                    Serdes.String()
                ),
                new MockRecordCollector()
            );
        context.setTime(1L);

        store.init((StateStoreContext) context, store);

        return store;
    }

    @Test
    public void shouldReturnNullIfKeyDoesNotExist() {
        assertNull(theStore.get("whatever"));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        assertThrows(NullPointerException.class, () -> theStore.get(null));
    }

    @Test
    public void shouldReturnValueOnRangeNullFromKey() {
        stubOneUnderlying.put("0", "zero");
        stubOneUnderlying.put("1", "one");
        stubOneUnderlying.put("2", "two");

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>("0", "zero"));
        expectedContents.add(new KeyValue<>("1", "one"));

        try (final KeyValueIterator<String, String> iterator = theStore.range(null, "1")) {
            assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnRangeNullToKey() {
        stubOneUnderlying.put("0", "zero");
        stubOneUnderlying.put("1", "one");
        stubOneUnderlying.put("2", "two");

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>("1", "one"));
        expectedContents.add(new KeyValue<>("2", "two"));

        try (final KeyValueIterator<String, String> iterator = theStore.range("1", null)) {
            assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPrefixScanNullPrefix() {
        assertThrows(NullPointerException.class, () -> theStore.prefixScan(null, new StringSerializer()));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnPrefixScanNullPrefixKeySerializer() {
        assertThrows(NullPointerException.class, () -> theStore.prefixScan("aa", null));
    }

    @Test
    public void shouldReturnValueOnReverseRangeNullFromKey() {
        stubOneUnderlying.put("0", "zero");
        stubOneUnderlying.put("1", "one");
        stubOneUnderlying.put("2", "two");

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>("1", "one"));
        expectedContents.add(new KeyValue<>("0", "zero"));

        try (final KeyValueIterator<String, String> iterator = theStore.reverseRange(null, "1")) {
            assertEquals(expectedContents, Utils.toList(iterator));
        }
    }

    @Test
    public void shouldReturnValueOnReverseRangeNullToKey() {
        stubOneUnderlying.put("0", "zero");
        stubOneUnderlying.put("1", "one");
        stubOneUnderlying.put("2", "two");

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(new KeyValue<>("2", "two"));
        expectedContents.add(new KeyValue<>("1", "one"));

        try (final KeyValueIterator<String, String> iterator = theStore.reverseRange("1", null)) {
            assertEquals(expectedContents, Utils.toList(iterator));
        }
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
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b")) {
            keyValueIterator.next();
            assertThrows(NoSuchElementException.class, keyValueIterator::next);
        }
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWhilePeekNext() {
        stubOneUnderlying.put("a", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b")) {
            keyValueIterator.next();
            assertThrows(NoSuchElementException.class, keyValueIterator::peekNextKey);
        }
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWhileNextForPrefixScan() {
        stubOneUnderlying.put("a", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.prefixScan("a", new StringSerializer())) {
            keyValueIterator.next();
            assertThrows(NoSuchElementException.class, keyValueIterator::next);
        }
    }

    @Test
    public void shouldThrowNoSuchElementExceptionWhilePeekNextForPrefixScan() {
        stubOneUnderlying.put("a", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.prefixScan("a", new StringSerializer())) {
            keyValueIterator.next();
            assertThrows(NoSuchElementException.class, keyValueIterator::peekNextKey);
        }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhileRemove() {
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.all()) {
            assertThrows(UnsupportedOperationException.class, keyValueIterator::remove);
        }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhileReverseRange() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.reverseRange("a", "b")) {
            assertThrows(UnsupportedOperationException.class, keyValueIterator::remove);
        }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhileRange() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.range("a", "b")) {
            assertThrows(UnsupportedOperationException.class, keyValueIterator::remove);
        }
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionWhilePrefixScan() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        try (final KeyValueIterator<String, String> keyValueIterator = theStore.prefixScan("a", new StringSerializer())) {
            assertThrows(UnsupportedOperationException.class, keyValueIterator::remove);
        }
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
    public void shouldSupportReverseRange() {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("c", "c");

        final List<KeyValue<String, String>> results = toList(theStore.reverseRange("a", "b"));
        assertArrayEquals(
            asList(
                new KeyValue<>("b", "b"),
                new KeyValue<>("a", "a")
            ).toArray(),
            results.toArray());
    }

    @Test
    public void shouldReturnKeysWithGivenPrefixExcludingNextKeyLargestKey() {
        stubOneUnderlying.put("abc", "a");
        stubOneUnderlying.put("abcd", "b");
        stubOneUnderlying.put("abce", "c");

        final List<KeyValue<String, String>> results = toList(theStore.prefixScan("abcd", new StringSerializer()));
        assertTrue(results.contains(new KeyValue<>("abcd", "b")));
        assertEquals(1, results.size());
    }

    @Test
    public void shouldSupportPrefixScan() {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("aa", "b");
        stubOneUnderlying.put("b", "c");

        final List<KeyValue<String, String>> results = toList(theStore.prefixScan("a", new StringSerializer()));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("aa", "b")));
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
        assertArrayEquals(
            asList(
                new KeyValue<>("a", "a"),
                new KeyValue<>("b", "b"),
                new KeyValue<>("c", "c"),
                new KeyValue<>("d", "d")
            ).toArray(),
            results.toArray());
    }

    @Test
    public void shouldSupportPrefixScanAcrossMultipleKVStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("aa", "c");
        cache.put("ab", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.prefixScan("a", new StringSerializer()));
        assertArrayEquals(
            asList(
                new KeyValue<>("a", "a"),
                new KeyValue<>("aa", "c"),
                new KeyValue<>("ab", "d")
            ).toArray(),
            results.toArray());
    }

    @Test
    public void shouldSupportReverseRangeAcrossMultipleKVStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.reverseRange("a", "e"));
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

    @Test
    public void shouldSupportReverseAllAcrossMultipleStores() {
        final KeyValueStore<String, String> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.reverseAll());
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertTrue(results.contains(new KeyValue<>("x", "x")));
        assertTrue(results.contains(new KeyValue<>("z", "z")));
        assertEquals(6, results.size());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().get("anything"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnApproximateNumEntriesDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().approximateNumEntries());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnRangeDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().range("anything", "something"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnReverseRangeDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().reverseRange("anything", "something"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnPrefixScanDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().prefixScan("anything", new StringSerializer()));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnAllDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().all());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnReverseAllDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().reverseAll());
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
        stubProviderTwo.addStore("my-storeA", new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }

    private CompositeReadOnlyKeyValueStore<Object, Object> rebalancing() {
        return new CompositeReadOnlyKeyValueStore<>(
            new WrappingStoreProvider(
                singletonList(new StateStoreProviderStub(true)),
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())),
            QueryableStoreTypes.keyValueStore(),
            storeName
        );
    }
}