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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDBSessionStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StoresTest {

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCreateInMemoryStoreSupplierWithLoggedConfig() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .inMemory()
                .enableLogging(Collections.singletonMap("retention.ms", "1000"))
                .build();

        final Map<String, String> config = supplier.logConfig();
        assertTrue(supplier.loggingEnabled());
        assertEquals("1000", config.get("retention.ms"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCreateInMemoryStoreSupplierNotLogged() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .inMemory()
                .disableLogging()
                .build();

        assertFalse(supplier.loggingEnabled());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCreatePersistenStoreSupplierWithLoggedConfig() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent()
                .enableLogging(Collections.singletonMap("retention.ms", "1000"))
                .build();

        final Map<String, String> config = supplier.logConfig();
        assertTrue(supplier.loggingEnabled());
        assertEquals("1000", config.get("retention.ms"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCreatePersistenStoreSupplierNotLogged() {
        final StateStoreSupplier supplier = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent()
                .disableLogging()
                .build();

        assertFalse(supplier.loggingEnabled());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTryingToConstructWindowStoreWithLessThanTwoSegments() {
        final Stores.PersistentKeyValueFactory<String, String> storeFactory = Stores.create("store")
                .withKeys(Serdes.String())
                .withValues(Serdes.String())
                .persistent();
        try {
            storeFactory.windowed(1, 1, 1, false);
            fail("Should have thrown illegal argument exception as number of segments is less than 2");
        } catch (final IllegalArgumentException e) {
         // ok
        }
    }

    @Test
    public void shouldCreateInMemoryKeyValueStore() {
        assertThat(Stores.inMemoryKeyValueStore("memory").get(), instanceOf(InMemoryKeyValueStore.class));
    }

    @Test
    public void shouldCreateMemoryNavigableCache() {
        assertThat(Stores.lruMap("map", 10).get(), instanceOf(MemoryNavigableLRUCache.class));
    }

    @Test
    public void shouldCreateRocksDbStore() {
        assertThat(Stores.persistentKeyValueStore("store").get(), instanceOf(RocksDBStore.class));
    }

    @Test
    public void shouldCreateRocksDbWindowStore() {
        assertThat(Stores.persistentWindowStore("store", 1, 3, 1, false).get(), instanceOf(RocksDBWindowStore.class));
    }

    @Test
    public void shouldCreateRocksDbSessionStore() {
        assertThat(Stores.persistentSessionStore("store", 1).get(), instanceOf(RocksDBSessionStore.class));
    }

    @Test
    public void shouldBuildWindowStore() {
        final WindowStore<String, String> store = Stores.windowStoreBuilder(Stores.persistentWindowStore("store", 3, 2, 3, true),
                                                                      Serdes.String(),
                                                                      Serdes.String()).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildKeyValueStore() {
        final KeyValueStore<String, String> store = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("name"),
                                                                          Serdes.String(),
                                                                          Serdes.String()).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildSessionStore() {
        final SessionStore<String, String> store = Stores.sessionStoreBuilder(Stores.persistentSessionStore("name", 10),
                                                                       Serdes.String(),
                                                                       Serdes.String()).build();
        assertThat(store, not(nullValue()));
    }
}