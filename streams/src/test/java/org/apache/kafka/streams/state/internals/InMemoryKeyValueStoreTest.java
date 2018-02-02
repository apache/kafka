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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class InMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(
            ProcessorContext context,
            Class<K> keyClass,
            Class<V> valueClass,
            boolean useContextSerdes) {

        StateStoreSupplier supplier;
        if (useContextSerdes) {
            supplier = Stores.create("my-store").withKeys(context.keySerde()).withValues(context.valueSerde()).inMemory().build();
        } else {
            supplier = Stores.create("my-store").withKeys(keyClass).withValues(valueClass).inMemory().build();
        }

        KeyValueStore<K, V> store = (KeyValueStore<K, V>) supplier.get();
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

        store = createKeyValueStore(driver.context(), Integer.class, String.class, true);
        context.restore(store.name(), driver.restoredEntries());

        assertEquals(3, driver.sizeOf(store));

        assertThat(store.get(0), nullValue());
    }
}
