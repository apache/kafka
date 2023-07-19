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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDBSegmentedBytesStore;
import org.apache.kafka.streams.state.internals.RocksDBSessionStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBTimestampedSegmentedBytesStore;
import org.apache.kafka.streams.state.internals.RocksDBTimestampedStore;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Test;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StoresTest {

    @Test
    public void shouldThrowIfPersistentKeyValueStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentKeyValueStore(null));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentTimestampedKeyValueStore(null));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfPersistentVersionedKeyValueStoreStoreNameIsNull() {
        Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentVersionedKeyValueStore(null, ZERO));
        assertEquals("name cannot be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> Stores.persistentVersionedKeyValueStore(null, ZERO, ofMillis(1)));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfPersistentVersionedKeyValueStoreHistoryRetentionIsNegative() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentVersionedKeyValueStore("anyName", ofMillis(-1)));
        assertEquals("historyRetention cannot be negative", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentVersionedKeyValueStore("anyName", ofMillis(-1), ofMillis(1)));
        assertEquals("historyRetention cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfPersistentVersionedKeyValueStoreSegmentIntervalIsZeroOrNegative() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentVersionedKeyValueStore("anyName", ZERO, ZERO));
        assertEquals("segmentInterval cannot be zero or negative", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentVersionedKeyValueStore("anyName", ZERO, ofMillis(-1)));
        assertEquals("segmentInterval cannot be zero or negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIMemoryKeyValueStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.inMemoryKeyValueStore(null));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfILruMapStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.lruMap(null, 0));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfILruMapStoreCapacityIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.lruMap("anyName", -1));
        assertEquals("maxCacheSize cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentWindowStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentWindowStore(null, ZERO, ZERO, false));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentTimestampedWindowStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentTimestampedWindowStore(null, ZERO, ZERO, false));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentWindowStore("anyName", ofMillis(-1L), ZERO, false));
        assertEquals("retentionPeriod cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentTimestampedWindowStore("anyName", ofMillis(-1L), ZERO, false));
        assertEquals("retentionPeriod cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false));
        assertEquals("windowSize cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentTimestampedWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false));
        assertEquals("windowSize cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentSessionStoreStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.persistentSessionStore(null, ofMillis(0)));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> Stores.persistentSessionStore("anyName", ofMillis(-1)));
        assertEquals("retentionPeriod cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfSupplierIsNullForWindowStoreBuilder() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.windowStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
        assertEquals("supplier cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfSupplierIsNullForKeyValueStoreBuilder() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.keyValueStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
        assertEquals("supplier cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfSupplierIsNullForVersionedKeyValueStoreBuilder() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.versionedKeyValueStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
        assertEquals("supplier cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfSupplierIsNullForSessionStoreBuilder() {
        final Exception e = assertThrows(NullPointerException.class, () -> Stores.sessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray()));
        assertEquals("supplier cannot be null", e.getMessage());
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
        assertThat(
            Stores.persistentKeyValueStore("store").get(),
            allOf(not(instanceOf(RocksDBTimestampedStore.class)), instanceOf(RocksDBStore.class)));
    }

    @Test
    public void shouldCreateRocksDbTimestampedStore() {
        assertThat(Stores.persistentTimestampedKeyValueStore("store").get(), instanceOf(RocksDBTimestampedStore.class));
    }

    @Test
    public void shouldCreateRocksDbVersionedStore() {
        final KeyValueStore<Bytes, byte[]> store = Stores.persistentVersionedKeyValueStore("store", ofMillis(1)).get();
        assertThat(store, instanceOf(VersionedBytesStore.class));
        assertThat(store.persistent(), equalTo(true));
    }

    @Test
    public void shouldCreateRocksDbWindowStore() {
        final WindowStore store = Stores.persistentWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBWindowStore.class));
        assertThat(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore.class)), instanceOf(RocksDBSegmentedBytesStore.class)));
    }

    @Test
    public void shouldCreateRocksDbTimestampedWindowStore() {
        final WindowStore store = Stores.persistentTimestampedWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBWindowStore.class));
        assertThat(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore.class));
    }

    @Test
    public void shouldCreateRocksDbSessionStore() {
        assertThat(Stores.persistentSessionStore("store", ofMillis(1)).get(), instanceOf(RocksDBSessionStore.class));
    }

    @Test
    public void shouldBuildKeyValueStore() {
        final KeyValueStore<String, String> store = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedKeyValueStoreThatWrapsKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedKeyValueStoreThatWrapsInMemoryKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store = Stores.timestampedKeyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).withLoggingDisabled().withCachingDisabled().build();
        assertThat(store, not(nullValue()));
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(TimestampedBytesStore.class));
    }

    @Test
    public void shouldBuildVersionedKeyValueStore() {
        final VersionedKeyValueStore<String, String> store = Stores.versionedKeyValueStoreBuilder(
            Stores.persistentVersionedKeyValueStore("name", ofMillis(1)),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildWindowStore() {
        final WindowStore<String, String> store = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedWindowStore() {
        final TimestampedWindowStore<String, String> store = Stores.timestampedWindowStoreBuilder(
            Stores.persistentTimestampedWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedWindowStoreThatWrapsWindowStore() {
        final TimestampedWindowStore<String, String> store = Stores.timestampedWindowStoreBuilder(
            Stores.persistentWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedWindowStoreThatWrapsInMemoryWindowStore() {
        final TimestampedWindowStore<String, String> store = Stores.timestampedWindowStoreBuilder(
            Stores.inMemoryWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).withLoggingDisabled().withCachingDisabled().build();
        assertThat(store, not(nullValue()));
        assertThat(((WrappedStateStore) store).wrapped(), instanceOf(TimestampedBytesStore.class));
    }

    @Test
    public void shouldBuildSessionStore() {
        final SessionStore<String, String> store = Stores.sessionStoreBuilder(
            Stores.persistentSessionStore("name", ofMillis(10)),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }
}
