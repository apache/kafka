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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.GenericInMemoryKeyValueStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FilteredCacheIteratorTest {

    private static final CacheFunction IDENTITY_FUNCTION = new CacheFunction() {
        @Override
        public Bytes key(final Bytes cacheKey) {
            return cacheKey;
        }

        @Override
        public Bytes cacheKey(final Bytes key) {
            return key;
        }
    };

    @SuppressWarnings("unchecked")
    private final KeyValueStore<Bytes, LRUCacheEntry> store = new GenericInMemoryKeyValueStore<>("my-store");
    private final KeyValue<Bytes, LRUCacheEntry> firstEntry = KeyValue.pair(Bytes.wrap("a".getBytes()),
                                                                            new LRUCacheEntry("1".getBytes()));
    private final List<KeyValue<Bytes, LRUCacheEntry>> entries = asList(
            firstEntry,
            KeyValue.pair(Bytes.wrap("b".getBytes()),
                          new LRUCacheEntry("2".getBytes())),
            KeyValue.pair(Bytes.wrap("c".getBytes()),
                          new LRUCacheEntry("3".getBytes())));

    private FilteredCacheIterator allIterator;
    private FilteredCacheIterator firstEntryIterator;

    @Before
    public void before() {
        store.putAll(entries);
        final HasNextCondition allCondition = new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                return iterator.hasNext();
            }
        };
        allIterator = new FilteredCacheIterator(
            new DelegatingPeekingKeyValueIterator<>("",
                                                    store.all()), allCondition, IDENTITY_FUNCTION);

        final HasNextCondition firstEntryCondition = new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                return iterator.hasNext() && iterator.peekNextKey().equals(firstEntry.key);
            }
        };
        firstEntryIterator = new FilteredCacheIterator(
                new DelegatingPeekingKeyValueIterator<>("",
                                                        store.all()), firstEntryCondition, IDENTITY_FUNCTION);

    }

    @Test
    public void shouldAllowEntryMatchingHasNextCondition() {
        final List<KeyValue<Bytes, LRUCacheEntry>> keyValues = toList(allIterator);
        assertThat(keyValues, equalTo(entries));
    }

    @Test
    public void shouldPeekNextKey() {
        while (allIterator.hasNext()) {
            final Bytes nextKey = allIterator.peekNextKey();
            final KeyValue<Bytes, LRUCacheEntry> next = allIterator.next();
            assertThat(next.key, equalTo(nextKey));
        }
    }

    @Test
    public void shouldPeekNext() {
        while (allIterator.hasNext()) {
            final KeyValue<Bytes, LRUCacheEntry> peeked = allIterator.peekNext();
            final KeyValue<Bytes, LRUCacheEntry> next = allIterator.next();
            assertThat(peeked, equalTo(next));
        }
    }

    @Test
    public void shouldNotHaveNextIfHasNextConditionNotMet() {
        assertTrue(firstEntryIterator.hasNext());
        firstEntryIterator.next();
        assertFalse(firstEntryIterator.hasNext());
    }

    @Test
    public void shouldFilterEntriesNotMatchingHasNextCondition() {
        final List<KeyValue<Bytes, LRUCacheEntry>> keyValues = toList(firstEntryIterator);
        assertThat(keyValues, equalTo(Collections.singletonList(firstEntry)));
    }

    @Test
    public void shouldThrowUnsupportedOperationExeceptionOnRemove() {
        assertThrows(UnsupportedOperationException.class, () -> allIterator.remove());
    }

}
