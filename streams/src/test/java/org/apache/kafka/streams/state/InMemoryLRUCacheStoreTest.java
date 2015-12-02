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
package org.apache.kafka.streams.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.junit.Test;

public class InMemoryLRUCacheStoreTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testPutGetRange() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create();
        StateStoreSupplier supplier = Stores.create("my-store")
                                                     .withIntegerKeys().withStringValues()
                                                     .inMemory().maxEntries(3)
                                                     .build();
        KeyValueStore<Integer, String> store = (KeyValueStore<Integer, String>) supplier.get();
        store.init(driver.context());

        // Verify that the store reads and writes correctly, keeping only the last 2 entries ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(3, "three");
        store.put(4, "four");
        store.put(5, "five");

        // It should only keep the last 4 added ...
        assertEquals(3, driver.sizeOf(store));
        assertNull(store.get(0));
        assertNull(store.get(1));
        assertNull(store.get(2));
        assertEquals("three", store.get(3));
        assertEquals("four", store.get(4));
        assertEquals("five", store.get(5));
        store.delete(5);

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        assertNull(driver.flushedEntryStored(0));
        assertNull(driver.flushedEntryStored(1));
        assertNull(driver.flushedEntryStored(2));
        assertEquals("three", driver.flushedEntryStored(3));
        assertEquals("four", driver.flushedEntryStored(4));
        assertNull(driver.flushedEntryStored(5));

        assertEquals(true, driver.flushedEntryRemoved(0));
        assertEquals(true, driver.flushedEntryRemoved(1));
        assertEquals(true, driver.flushedEntryRemoved(2));
        assertEquals(false, driver.flushedEntryRemoved(3));
        assertEquals(false, driver.flushedEntryRemoved(4));
        assertEquals(true, driver.flushedEntryRemoved(5));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPutGetRangeWithDefaultSerdes() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create();

        Serializer<Integer> keySer = (Serializer<Integer>) driver.context().keySerializer();
        Deserializer<Integer> keyDeser = (Deserializer<Integer>) driver.context().keyDeserializer();
        Serializer<String> valSer = (Serializer<String>) driver.context().valueSerializer();
        Deserializer<String> valDeser = (Deserializer<String>) driver.context().valueDeserializer();
        StateStoreSupplier supplier = Stores.create("my-store")
                                                     .withKeys(keySer, keyDeser)
                                                     .withValues(valSer, valDeser)
                                                     .inMemory().maxEntries(3)
                                                     .build();
        KeyValueStore<Integer, String> store = (KeyValueStore<Integer, String>) supplier.get();
        store.init(driver.context());

        // Verify that the store reads and writes correctly, keeping only the last 2 entries ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(3, "three");
        store.put(4, "four");
        store.put(5, "five");

        // It should only keep the last 4 added ...
        assertEquals(3, driver.sizeOf(store));
        assertNull(store.get(0));
        assertNull(store.get(1));
        assertNull(store.get(2));
        assertEquals("three", store.get(3));
        assertEquals("four", store.get(4));
        assertEquals("five", store.get(5));
        store.delete(5);

        // Flush the store and verify all current entries were properly flushed ...
        store.flush();
        assertNull(driver.flushedEntryStored(0));
        assertNull(driver.flushedEntryStored(1));
        assertNull(driver.flushedEntryStored(2));
        assertEquals("three", driver.flushedEntryStored(3));
        assertEquals("four", driver.flushedEntryStored(4));
        assertNull(driver.flushedEntryStored(5));

        assertEquals(true, driver.flushedEntryRemoved(0));
        assertEquals(true, driver.flushedEntryRemoved(1));
        assertEquals(true, driver.flushedEntryRemoved(2));
        assertEquals(false, driver.flushedEntryRemoved(3));
        assertEquals(false, driver.flushedEntryRemoved(4));
        assertEquals(true, driver.flushedEntryRemoved(5));
    }

    @Test
    public void testRestore() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(4, "four");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        StateStoreSupplier supplier = Stores.create("my-store")
                                                     .withIntegerKeys().withStringValues()
                                                     .inMemory().maxEntries(3)
                                                     .build();
        KeyValueStore<Integer, String> store = (KeyValueStore<Integer, String>) supplier.get();
        store.init(driver.context());

        // Verify that the store's contents were properly restored ...
        assertEquals(0, driver.checkForRestoredEntries(store));

        // and there are no other entries ...
        assertEquals(3, driver.sizeOf(store));
    }

}
