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

import java.nio.file.Files;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import static org.apache.kafka.streams.state.internals.InMemoryKeyValueStore.STORE_EXTENSION;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class InMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {
    public static final String STORE = "my-store";

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(STORE),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    @SuppressWarnings("unchecked")
    private <K, V> KeyValueStore<K, V> createPersistedKeyValueStore() {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(STORE, true),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    @Test
    public void shouldRemoveKeysWithNullValues() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");
        driver.addEntryToRestoreLog(0, null);

        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        assertEquals(3, driver.sizeOf(store));

        assertThat(store.get(0), nullValue());
    }

    @Test
    public void shouldPersistAndRestore() throws Exception {
        store.close();

        // Delete checkpoint file.
        Files.deleteIfExists(context.stateDir().toPath().resolve(STORE + STORE_EXTENSION));

        // Add any entries that will be restored to any store that uses the driver's context ...
        final KeyValueStore<Integer, String> store = createPersistedKeyValueStore();

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(3, "three");

        for (int i = 0; i < InMemoryKeyValueStore.COUNT_FLUSH_TO_STORE; i++)
            store.flush();

        // Checkpoint should be written to the disk.
        TestUtils.waitForCondition(() -> Files.exists(context.stateDir().toPath().resolve(STORE + STORE_EXTENSION)),
            "store should be persisted");

        store.close();

        // Store state should be restored.
        final KeyValueStore<Integer, String> restored = createPersistedKeyValueStore();

        assertEquals(4, restored.approximateNumEntries());
        assertEquals("zero", restored.get(0));
        assertEquals("one", restored.get(1));
        assertEquals("two", restored.get(2));
        assertEquals("three", restored.get(3));

        restored.put(4, "four");

        for (int i = 0; i < InMemoryKeyValueStore.COUNT_FLUSH_TO_STORE; i++)
            restored.flush();

        // New checkpoint should be written to the disk.
        TestUtils.waitForCondition(() -> "four".equals(createPersistedKeyValueStore().get(4)),
            "store should be rewritten");
    }

    @Test
    public void shouldSkipCorruptedFile() throws Exception {
        store.close();

        Files.deleteIfExists(context.stateDir().toPath().resolve(STORE + STORE_EXTENSION));
        Files.createFile(context.stateDir().toPath().resolve(STORE + STORE_EXTENSION));

        // Store should be empty.
        final KeyValueStore<Integer, String> restored = createPersistedKeyValueStore();

        assertEquals(0, restored.approximateNumEntries());
        assertNull(restored.get(0));
        assertNull(restored.get(1));
        assertNull(restored.get(2));
        assertNull(restored.get(3));
    }
}
