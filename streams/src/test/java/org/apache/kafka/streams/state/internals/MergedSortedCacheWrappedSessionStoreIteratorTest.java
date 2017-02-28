/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MergedSortedCacheWrappedSessionStoreIteratorTest {

    private final String storeKey = "a";
    private final String cacheKey = "b";

    private final SessionWindow storeWindow = new SessionWindow(0, 1);
    private final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
            KeyValue.pair(new Windowed<>(Bytes.wrap(storeKey.getBytes()), storeWindow), storeKey.getBytes())).iterator();
    private final SessionWindow cacheWindow = new SessionWindow(10, 20);
    private final Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(KeyValue.pair(
            SessionKeySerde.toBinary(
                    new Windowed<>(cacheKey, cacheWindow), Serdes.String().serializer()), new LRUCacheEntry(cacheKey.getBytes())))
            .iterator();

    @Test
    public void shouldHaveNextFromStore() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(storeKvs, Collections.<KeyValue<Bytes, LRUCacheEntry>>emptyIterator());
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromStore() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(storeKvs, Collections.<KeyValue<Bytes, LRUCacheEntry>>emptyIterator());
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
    }

    @Test
    public void shouldPeekNextKeyFromStore() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(storeKvs, Collections.<KeyValue<Bytes, LRUCacheEntry>>emptyIterator());
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(storeKey, storeWindow)));
    }

    @Test
    public void shouldHaveNextFromCache() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(Collections.<KeyValue<Windowed<Bytes>, byte[]>>emptyIterator(),
                                 cacheKvs);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromCache() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(Collections.<KeyValue<Windowed<Bytes>, byte[]>>emptyIterator(), cacheKvs);
        assertThat(mergeIterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
    }

    @Test
    public void shouldPeekNextKeyFromCache() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> mergeIterator
                = createIterator(Collections.<KeyValue<Windowed<Bytes>, byte[]>>emptyIterator(), cacheKvs);
        assertThat(mergeIterator.peekNextKey(), equalTo(new Windowed<>(cacheKey, cacheWindow)));
    }

    @Test
    public void shouldIterateBothStoreAndCache() throws Exception {
        final MergedSortedCacheSessionStoreIterator<String, String> iterator = createIterator(storeKvs, cacheKvs);
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
        assertThat(iterator.next(), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
        assertFalse(iterator.hasNext());
    }

    private MergedSortedCacheSessionStoreIterator<String, String> createIterator(final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs,
                                                                                 final Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs) {
        final DelegatingPeekingKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator
                = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator
                = new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
        return new MergedSortedCacheSessionStoreIterator<>(cacheIterator, storeIterator, new StateSerdes<>("name", Serdes.String(), Serdes.String()));
    }

}