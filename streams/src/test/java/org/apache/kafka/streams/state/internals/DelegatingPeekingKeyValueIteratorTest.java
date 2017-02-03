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
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DelegatingPeekingKeyValueIteratorTest {

    private final String name = "name";
    private InMemoryKeyValueStore<String, String> store;

    @Before
    public void setUp() throws Exception {
        store = new InMemoryKeyValueStore<>(name, Serdes.String(), Serdes.String());
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        store.put("A", "A");
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        assertEquals("A", peekingIterator.peekNextKey());
        assertEquals("A", peekingIterator.peekNextKey());
        assertTrue(peekingIterator.hasNext());
    }

    @Test
    public void shouldPeekNext() throws Exception {
        store.put("A", "A");
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        assertEquals(KeyValue.pair("A", "A"), peekingIterator.peekNext());
        assertEquals(KeyValue.pair("A", "A"), peekingIterator.peekNext());
        assertTrue(peekingIterator.hasNext());
    }

    @Test
    public void shouldPeekAndIterate() throws Exception {
        final String[] kvs = {"a", "b", "c", "d", "e", "f"};
        for (String kv : kvs) {
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
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled() throws Exception {
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        peekingIterator.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled() throws Exception {
        final DelegatingPeekingKeyValueIterator<String, String> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        peekingIterator.peekNextKey();
    }


}