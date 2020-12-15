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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.GenericInMemoryKeyValueStore;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DelegatingPeekingKeyValueIteratorTest {

    private final String name = "name";
    private KeyValueStore<String, String> store;

    @Before
    public void setUp() {
        store = new GenericInMemoryKeyValueStore<>(name);
    }

    @Test
    public void shouldPeekNextKey() {
        store.put("A", "A");
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        assertEquals("A", peekingIterator.peekNextKey());
        assertEquals("A", peekingIterator.peekNextKey());
        assertTrue(peekingIterator.hasNext());
        peekingIterator.close();
    }

    @Test
    public void shouldPeekNext() {
        store.put("A", "A");
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        assertEquals(KeyValue.pair("A", "A"), peekingIterator.peekNext());
        assertEquals(KeyValue.pair("A", "A"), peekingIterator.peekNext());
        assertTrue(peekingIterator.hasNext());
        peekingIterator.close();
    }

    @Test
    public void shouldPeekAndIterate() {
        final String[] kvs = {"a", "b", "c", "d", "e", "f"};
        for (final String kv : kvs) {
            store.put(kv, kv);
        }

        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        int index = 0;
        while (peekingIterator.hasNext()) {
            final String peekNext = peekingIterator.peekNextKey();
            final String key = peekingIterator.next().key;
            assertEquals(kvs[index], peekNext);
            assertEquals(kvs[index], key);
            index++;
        }
        assertEquals(kvs.length, index);
        peekingIterator.close();
    }

    @Test
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled() {
        try (final DelegatingPeekingKeyValueIterator<String, String> peekingIterator =
            new DelegatingPeekingKeyValueIterator<>(name, store.all())) {
            assertThrows(NoSuchElementException.class, peekingIterator::next);
        }
    }

    @Test
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled() {
        try (final DelegatingPeekingKeyValueIterator<String, String> peekingIterator =
            new DelegatingPeekingKeyValueIterator<>(name, store.all())) {
            assertThrows(NoSuchElementException.class, peekingIterator::peekNextKey);
        }
    }


}