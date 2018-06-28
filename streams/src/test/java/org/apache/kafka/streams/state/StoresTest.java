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
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDBSessionStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class StoresTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfPersistentKeyValueStoreStoreNameIsNull() {
        Stores.persistentKeyValueStore(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIMemoryKeyValueStoreStoreNameIsNull() {
        Stores.inMemoryKeyValueStore(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfILruMapStoreNameIsNull() {
        Stores.lruMap(null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfILruMapStoreCapacityIsNegative() {
        Stores.lruMap("anyName", -1);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIPersistentWindowStoreStoreNameIsNull() {
        Stores.persistentWindowStore(null, 0L, 0L, false, 60_000L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative() {
        Stores.persistentWindowStore("anyName", -1L, 0L, false, 60_000L);
    }

    @Deprecated
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne() {
        Stores.persistentWindowStore("anyName", 0L, 1, 0L, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative() {
        Stores.persistentWindowStore("anyName", 0L, -1L, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreIfSegmentIntervalIsTooSmall() {
        Stores.persistentWindowStore("anyName", 1L, 1L, false, 59_999L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIPersistentSessionStoreStoreNameIsNull() {
        Stores.persistentSessionStore(null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative() {
        Stores.persistentSessionStore("anyName", -1);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForWindowStoreBuilder() {
        Stores.windowStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForKeyValueStoreBuilder() {
        Stores.keyValueStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForSessionStoreBuilder() {
        Stores.sessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
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
        assertThat(Stores.persistentWindowStore("store", 1L, 1L, false).get(), instanceOf(RocksDBWindowStore.class));
    }

    @Test
    public void shouldCreateRocksDbSessionStore() {
        assertThat(Stores.persistentSessionStore("store", 1).get(), instanceOf(RocksDBSessionStore.class));
    }

    @Test
    public void shouldBuildWindowStore() {
        final WindowStore<String, String> store = Stores.windowStoreBuilder(Stores.persistentWindowStore("store", 3L, 3L, true),
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