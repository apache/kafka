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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MergedSortedCacheWrappedSessionStoreIteratorTest {

    private static final SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1) {
        @Override
        public long segmentId(final Bytes key) {
            return 0;
        }
    };

    private final Bytes storeKey = Bytes.wrap("a".getBytes());
    private final Bytes cacheKey = Bytes.wrap("b".getBytes());

    private final SessionWindow storeWindow = new SessionWindow(0, 1);
    private final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
            KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey.get())).iterator();
    private final SessionWindow cacheWindow = new SessionWindow(10, 20);
    private final Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(
        KeyValue.pair(
            SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(SessionKeySchema.toBinary(new Windowed<>(cacheKey, cacheWindow))),
            new LRUCacheEntry(cacheKey.get())
        )).iterator();

    @Test
    public void shouldHaveNextFromStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), false);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldHaveNextFromReverseStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), true);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), false);
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey.get())));
    }

    @Test
    public void shouldGetNextFromReverseStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), true);
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey.get())));
    }

    @Test
    public void shouldPeekNextKeyFromStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), false);
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(storeKey, storeWindow)));
    }

    @Test
    public void shouldPeekNextKeyFromReverseStore() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator(), true);
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(storeKey, storeWindow)));
    }

    @Test
    public void shouldHaveNextFromCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldHaveNextFromReverseCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
    }

    @Test
    public void shouldGetNextFromReverseCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
    }

    @Test
    public void shouldPeekNextKeyFromCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(cacheKey, cacheWindow)));
    }

    @Test
    public void shouldPeekNextKeyFromReverseCache() {
        final MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(cacheKey, cacheWindow)));
    }

    @Test
    public void shouldIterateBothStoreAndCache() {
        final MergedSortedCacheSessionStoreIterator iterator = createIterator(storeKvs, cacheKvs, true);
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey.get())));
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReverseIterateBothStoreAndCache() {
        final MergedSortedCacheSessionStoreIterator iterator = createIterator(storeKvs, cacheKvs, false);
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey.get())));
        assertFalse(iterator.hasNext());
    }

    private MergedSortedCacheSessionStoreIterator createIterator(final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs,
                                                                 final Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs,
                                                                 final boolean forward) {
        final DelegatingPeekingKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
            new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
        return new MergedSortedCacheSessionStoreIterator(cacheIterator, storeIterator, SINGLE_SEGMENT_CACHE_FUNCTION, forward);
    }

}
