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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

public class InMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {
    public static final String STORE = "my-store";

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext<Object, Object> context) {
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

        persistStoreAndCheck();

        checkRestored();
    }

    @Test
    public void shouldSkipCorruptedFile() throws Exception {
        store.close();

        persistStoreAndCheck();

        // +1 to ensure that "future" is bigger then saved timestamp.
        final long future = System.currentTimeMillis() + 1;

        // Creating corrupted file from future.
        Files.createFile(context.stateDir().toPath()
            .resolve(STORE)
            .resolve(future + STORE_EXTENSION));

        checkRestored();
    }

    private void checkRestored() {
        // Store state should be restored.
        final KeyValueStore<Integer, String> restored = createPersistedKeyValueStore();

        assertEquals(4, restored.approximateNumEntries());
        assertEquals("zero", restored.get(0));
        assertEquals("one", restored.get(1));
        assertEquals("two", restored.get(2));
        assertEquals("three", restored.get(3));
    }

    private void persistStoreAndCheck() throws IOException, InterruptedException {
        // Clean directory.
        Files.deleteIfExists(context.stateDir().toPath().resolve(STORE));

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        final KeyValueStore<Integer, String> store = createPersistedKeyValueStore();

        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(3, "three");

        for (int i = 0; i < InMemoryKeyValueStore.COUNT_FLUSH_TO_STORE; i++)
            store.flush();

        // Store should be persisted to the disk.
        TestUtils.waitForCondition(() -> {
            final Path storeDir = context.stateDir().toPath().resolve(store.name());

            if (!Files.exists(storeDir))
                return false;

            return Files.list(storeDir).filter(p -> p.toString().endsWith(STORE_EXTENSION)).count() == 1;
        }, "store should be persisted");

        store.close();
    }
}
