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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.test.StateStoreProviderStub;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class CompositeReadOnlyWindowStoreTest {

    private static final long WINDOW_SIZE = 30_000;

    private final String storeName = "window-store";
    private StateStoreProviderStub stubProviderOne;
    private StateStoreProviderStub stubProviderTwo;
    private CompositeReadOnlyWindowStore<String, String> windowStore;
    private ReadOnlyWindowStoreStub<String, String> underlyingWindowStore;
    private ReadOnlyWindowStoreStub<String, String> otherUnderlyingStore;

    @Before
    public void before() {
        stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);
        underlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderOne.addStore(storeName, underlyingWindowStore);

        otherUnderlyingStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderOne.addStore("other-window-store", otherUnderlyingStore);

        windowStore = new CompositeReadOnlyWindowStore<>(
            new WrappingStoreProvider(asList(stubProviderOne, stubProviderTwo), StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore())),
            QueryableStoreTypes.windowStore(),
            storeName
        );
    }

    @Test
    public void shouldFetchValuesFromWindowStore() {
        underlyingWindowStore.put("my-key", "my-value", 0L);
        underlyingWindowStore.put("my-key", "my-later-value", 10L);

        assertEquals(
                asList(new KeyValue<>(0L, "my-value"), new KeyValue<>(10L, "my-later-value")),
                StreamsTestUtils.toList(windowStore.fetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L)))
        );
    }


    @Test
    public void shouldBackwardFetchValuesFromWindowStore() {
        underlyingWindowStore.put("my-key", "my-value", 0L);
        underlyingWindowStore.put("my-key", "my-later-value", 10L);

        assertEquals(
                asList(new KeyValue<>(10L, "my-later-value"), new KeyValue<>(0L, "my-value")),
                StreamsTestUtils.toList(windowStore.backwardFetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L)))
        );
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoData() {
        try (final WindowStoreIterator<String> iterator =
                 windowStore.fetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L))) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldReturnBackwardEmptyIteratorIfNoData() {
        try (final WindowStoreIterator<String> iterator =
                 windowStore.backwardFetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L))) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);

        underlyingWindowStore.put("key-one", "value-one", 0L);
        secondUnderlying.put("key-two", "value-two", 10L);

        final List<KeyValue<Long, String>> keyOneResults =
            StreamsTestUtils.toList(windowStore.fetch("key-one", ofEpochMilli(0L), ofEpochMilli(1L)));
        final List<KeyValue<Long, String>> keyTwoResults =
            StreamsTestUtils.toList(windowStore.fetch("key-two", ofEpochMilli(10L), ofEpochMilli(11L)));

        assertEquals(Collections.singletonList(KeyValue.pair(0L, "value-one")), keyOneResults);
        assertEquals(Collections.singletonList(KeyValue.pair(10L, "value-two")), keyTwoResults);
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStoresBackwards() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);

        underlyingWindowStore.put("key-one", "value-one", 0L);
        secondUnderlying.put("key-two", "value-two", 10L);

        final List<KeyValue<Long, String>> keyOneResults =
            StreamsTestUtils.toList(windowStore.backwardFetch("key-one", ofEpochMilli(0L), ofEpochMilli(1L)));
        final List<KeyValue<Long, String>> keyTwoResults =
            StreamsTestUtils.toList(windowStore.backwardFetch("key-two", ofEpochMilli(10L), ofEpochMilli(11L)));

        assertEquals(Collections.singletonList(KeyValue.pair(0L, "value-one")), keyOneResults);
        assertEquals(Collections.singletonList(KeyValue.pair(10L, "value-two")), keyTwoResults);
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put("some-key", "some-value", 0L);
        underlyingWindowStore.put("some-key", "my-value", 1L);

        final List<KeyValue<Long, String>> results =
            StreamsTestUtils.toList(windowStore.fetch("some-key", ofEpochMilli(0L), ofEpochMilli(2L)));
        assertEquals(Collections.singletonList(new KeyValue<>(1L, "my-value")), results);
    }

    @Test
    public void shouldNotGetValuesBackwardFromOtherStores() {
        otherUnderlyingStore.put("some-key", "some-value", 0L);
        underlyingWindowStore.put("some-key", "my-value", 1L);

        final List<KeyValue<Long, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetch("some-key", ofEpochMilli(0L), ofEpochMilli(2L)));
        assertEquals(Collections.singletonList(new KeyValue<>(1L, "my-value")), results);
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionOnRebalance() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any()))
            .thenThrow(new InvalidStateStoreException("store is unavailable"));

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );

        assertThrows(InvalidStateStoreException.class, () -> store.fetch("key", ofEpochMilli(1), ofEpochMilli(10)));
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionOnRebalanceWhenBackwards() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any()))
            .thenThrow(new InvalidStateStoreException("store is unavailable"));

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        assertThrows(InvalidStateStoreException.class, () -> store.backwardFetch("key", ofEpochMilli(1), ofEpochMilli(10)));
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionIfFetchThrows() {
        underlyingWindowStore.setOpen(false);
        final CompositeReadOnlyWindowStore<Object, Object> store =
            new CompositeReadOnlyWindowStore<>(
                new WrappingStoreProvider(singletonList(stubProviderOne), StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.windowStore())),
                QueryableStoreTypes.windowStore(),
                "window-store"
            );
        try {
            store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));
            Assert.fail("InvalidStateStoreException was expected");
        } catch (final InvalidStateStoreException e) {
            Assert.assertEquals("State store is not available anymore and may have been migrated to another instance; " +
                "please re-discover its location from the state metadata.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowInvalidStateStoreExceptionIfBackwardFetchThrows() {
        underlyingWindowStore.setOpen(false);
        final CompositeReadOnlyWindowStore<Object, Object> store =
            new CompositeReadOnlyWindowStore<>(
                new WrappingStoreProvider(singletonList(stubProviderOne), StoreQueryParameters.fromNameAndType("window-store", QueryableStoreTypes.windowStore())),
                QueryableStoreTypes.windowStore(),
                "window-store"
            );
        try {
            store.backwardFetch("key", ofEpochMilli(1), ofEpochMilli(10));
            Assert.fail("InvalidStateStoreException was expected");
        } catch (final InvalidStateStoreException e) {
            Assert.assertEquals("State store is not available anymore and may have been migrated to another instance; " +
                "please re-discover its location from the state metadata.", e.getMessage());
        }
    }

    @Test
    public void emptyBackwardIteratorAlwaysReturnsFalse() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator =
                 store.backwardFetch("key", ofEpochMilli(1), ofEpochMilli(10))) {

            Assert.assertFalse(windowStoreIterator.hasNext());
        }
    }

    @Test
    public void emptyIteratorAlwaysReturnsFalse() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator =
                 store.fetch("key", ofEpochMilli(1), ofEpochMilli(10))) {

            Assert.assertFalse(windowStoreIterator.hasNext());
        }
    }

    @Test
    public void emptyBackwardIteratorPeekNextKeyShouldThrowNoSuchElementException() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator = store.backwardFetch("key", ofEpochMilli(1), ofEpochMilli(10))) {
            assertThrows(NoSuchElementException.class, windowStoreIterator::peekNextKey);
        }
    }


    @Test
    public void emptyIteratorPeekNextKeyShouldThrowNoSuchElementException() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator =
                 store.fetch("key", ofEpochMilli(1), ofEpochMilli(10))) {
            assertThrows(NoSuchElementException.class, windowStoreIterator::peekNextKey);
        }
    }

    @Test
    public void emptyIteratorNextShouldThrowNoSuchElementException() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator =
                 store.fetch("key", ofEpochMilli(1), ofEpochMilli(10))) {
            assertThrows(NoSuchElementException.class, windowStoreIterator::next);
        }
    }

    @Test
    public void emptyBackwardIteratorNextShouldThrowNoSuchElementException() {
        final StateStoreProvider storeProvider = mock(StateStoreProvider.class);
        when(storeProvider.stores(anyString(), any())).thenReturn(emptyList());

        final CompositeReadOnlyWindowStore<Object, Object> store = new CompositeReadOnlyWindowStore<>(
            storeProvider,
            QueryableStoreTypes.windowStore(),
            "foo"
        );
        try (final WindowStoreIterator<Object> windowStoreIterator =
                 store.backwardFetch("key", ofEpochMilli(1), ofEpochMilli(10))) {
            assertThrows(NoSuchElementException.class, windowStoreIterator::next);
        }
    }

    @Test
    public void shouldFetchKeyRangeAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.fetch("a", "b", ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldFetchKeyRangeAcrossStoresWithNullKeyTo() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.fetch("a", null, ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"))));
    }

    @Test
    public void shouldFetchKeyRangeAcrossStoresWithNullKeyFrom() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.fetch(null, "c", ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"))));
    }

    @Test
    public void shouldFetchKeyRangeAcrossStoresWithNullKeyFromKeyTo() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.fetch(null, null, ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"))));
    }

    @Test
    public void shouldBackwardFetchKeyRangeAcrossStoresWithNullKeyTo() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetch("a", null, ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldBackwardFetchKeyRangeAcrossStoresWithNullKeyFrom() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetch(null, "c", ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b")
            )));
    }

    @Test
    public void shouldBackwardFetchKeyRangeAcrossStoresWithNullKeyFromKeyTo() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        secondUnderlying.put("c", "c", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetch(null, null, ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("c", new TimeWindow(10, 10 + WINDOW_SIZE)), "c"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldBackwardFetchKeyRangeAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetch("a", "b", ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldFetchKeyValueAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlyingWindowStore);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlyingWindowStore.put("b", "b", 10L);
        assertThat(windowStore.fetch("a", 0L), equalTo("a"));
        assertThat(windowStore.fetch("b", 10L), equalTo("b"));
        assertThat(windowStore.fetch("c", 10L), equalTo(null));
        assertThat(windowStore.fetch("a", 10L), equalTo(null));
    }

    @Test
    public void shouldGetAllAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results = StreamsTestUtils.toList(windowStore.all());
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldGetBackwardAllAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results = StreamsTestUtils.toList(windowStore.backwardAll());
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldFetchAllAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldBackwardFetchAllAcrossStores() {
        final ReadOnlyWindowStoreStub<String, String> secondUnderlying = new
            ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingWindowStore.put("a", "a", 0L);
        secondUnderlying.put("b", "b", 10L);
        final List<KeyValue<Windowed<String>, String>> results =
            StreamsTestUtils.toList(windowStore.backwardFetchAll(ofEpochMilli(0), ofEpochMilli(10)));
        assertThat(results, equalTo(Arrays.asList(
            KeyValue.pair(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
            KeyValue.pair(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
    }

    @Test
    public void shouldThrowNPEIfKeyIsNull() {
        assertThrows(NullPointerException.class, () -> windowStore.fetch(null, ofEpochMilli(0), ofEpochMilli(0)));
    }
}
