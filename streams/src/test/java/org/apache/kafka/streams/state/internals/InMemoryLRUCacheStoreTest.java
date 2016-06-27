/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryLRUCacheStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(
            ProcessorContext context,
            Class<K> keyClass,
            Class<V> valueClass,
            boolean useContextSerdes) {

        StateStoreSupplier supplier;
        if (useContextSerdes) {
            supplier = Stores.create("my-store").withKeys(context.keySerde()).withValues(context.valueSerde()).inMemory().maxEntries(10).build();
        } else {
            supplier = Stores.create("my-store").withKeys(keyClass).withValues(valueClass).inMemory().maxEntries(10).build();
        }

        KeyValueStore<K, V> store = (KeyValueStore<K, V>) supplier.get();
        store.init(context, store);
        return store;
    }

    @Test
    public void testEvict() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        KeyValueStore<Integer, String> store = createKeyValueStore(driver.context(), Integer.class, String.class, false);

        try {
            store.put(0, "zero");
            store.put(1, "one");
            store.put(2, "two");
            store.put(3, "three");
            store.put(4, "four");
            store.put(5, "five");
            store.put(6, "six");
            store.put(7, "seven");
            store.put(8, "eight");
            store.put(9, "nine");
            assertEquals(10, driver.sizeOf(store));

            store.put(10, "ten");
            store.flush();
            assertEquals(10, driver.sizeOf(store));
            assertTrue(driver.flushedEntryRemoved(0));
            assertEquals(1, driver.numFlushedEntryRemoved());

            store.delete(1);
            store.flush();
            assertEquals(9, driver.sizeOf(store));
            assertTrue(driver.flushedEntryRemoved(0));
            assertTrue(driver.flushedEntryRemoved(1));
            assertEquals(2, driver.numFlushedEntryRemoved());

            store.put(11, "eleven");
            store.flush();
            assertEquals(10, driver.sizeOf(store));
            assertEquals(2, driver.numFlushedEntryRemoved());

            store.put(2, "two-again");
            store.flush();
            assertEquals(10, driver.sizeOf(store));
            assertEquals(2, driver.numFlushedEntryRemoved());

            store.put(12, "twelve");
            store.flush();
            assertEquals(10, driver.sizeOf(store));
            assertTrue(driver.flushedEntryRemoved(0));
            assertTrue(driver.flushedEntryRemoved(1));
            assertTrue(driver.flushedEntryRemoved(2));
            assertEquals(3, driver.numFlushedEntryRemoved());
        } finally {
            store.close();
        }
    }
}
