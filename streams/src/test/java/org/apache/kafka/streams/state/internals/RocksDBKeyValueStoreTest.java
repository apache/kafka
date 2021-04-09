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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("my-store"),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    @Test
    public void shouldPerformRangeQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> range = store.range(1, 2);
        assertEquals("hi", range.next().value);
        assertEquals("goodbye", range.next().value);
        assertFalse(range.hasNext());
    }

    @Test
    public void shouldPerformAllQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> range = store.all();
        assertEquals("hi", range.next().value);
        assertEquals("goodbye", range.next().value);
        assertFalse(range.hasNext());
    }

    @Test
    public void shouldCloseOpenRangeIteratorsWhenStoreClosedAndThrowInvalidStateStoreOnHasNextAndNext() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> iteratorOne = store.range(1, 5);
        final KeyValueIterator<Integer, String> iteratorTwo = store.range(1, 4);

        assertTrue(iteratorOne.hasNext());
        assertTrue(iteratorTwo.hasNext());

        store.close();

        try {
            iteratorOne.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorOne.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (final InvalidStateStoreException e) {
            // ok
        }
    }
}
