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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Runs the full {@link AbstractWindowBytesStoreTest} suite against the in-memory window store with
 * transactional state stores enabled, so the staged-write/merge path is held to the same behavioural
 * contract as the non-transactional store.
 */
public class InMemoryTransactionalWindowStoreTest extends AbstractWindowBytesStoreTest {

    private static final String STORE_NAME = "InMemoryTransactionalWindowStore";

    @Override
    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                              final long windowSize,
                                              final boolean retainDuplicates,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(
                STORE_NAME,
                ofMillis(retentionPeriod),
                ofMillis(windowSize),
                retainDuplicates),
            keySerde,
            valueSerde)
            .build();
    }

    @Override
    boolean transactional() {
        return true;
    }

    /**
     * Transactional reads are snapshot-isolated: an open iterator reflects the staged state as of when
     * it was opened and does not observe records staged afterwards (the non-transactional store iterates
     * the live map and does observe them). It must still not throw. This overrides the base test's
     * live-iteration expectation accordingly.
     */
    @Test
    @Override
    public void shouldNotThrowConcurrentModificationException() {
        long currentTime = 0;
        windowStore.put(1, "one", currentTime);

        currentTime += WINDOW_SIZE * 10;
        windowStore.put(1, "two", currentTime);

        try (final KeyValueIterator<Windowed<Integer>, String> iterator = windowStore.all()) {
            currentTime += WINDOW_SIZE * 10;
            windowStore.put(1, "three", currentTime);

            currentTime += WINDOW_SIZE * 10;
            windowStore.put(2, "four", currentTime);

            assertEquals(windowedPair(1, "one", 0), iterator.next());
            assertEquals(windowedPair(1, "two", WINDOW_SIZE * 10), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldReadAcrossCommitBoundaries() {
        windowStore.put(1, "one", 0);                          // staged
        windowStore.commit(Collections.emptyMap());            // -> committed
        windowStore.put(1, "two", WINDOW_SIZE * 10);           // staged on top of committed

        // committed (one) and staged (two) are both visible before commit, and after
        try (final WindowStoreIterator<String> it = windowStore.fetch(1, 0, WINDOW_SIZE * 10)) {
            assertEquals(new KeyValue<>(0L, "one"), it.next());
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two"), it.next());
            assertFalse(it.hasNext());
        }
        windowStore.commit(Collections.emptyMap());
        try (final WindowStoreIterator<String> it = windowStore.fetch(1, 0, WINDOW_SIZE * 10)) {
            assertEquals(new KeyValue<>(0L, "one"), it.next());
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two"), it.next());
            assertFalse(it.hasNext());
        }

        // a staged tombstone hides the committed record, and stays hidden after commit
        windowStore.put(1, null, 0);
        try (final WindowStoreIterator<String> it = windowStore.fetch(1, 0, WINDOW_SIZE * 10)) {
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two"), it.next());
            assertFalse(it.hasNext());
        }
        windowStore.commit(Collections.emptyMap());
        try (final WindowStoreIterator<String> it = windowStore.fetch(1, 0, WINDOW_SIZE * 10)) {
            assertEquals(new KeyValue<>(WINDOW_SIZE * 10, "two"), it.next());
            assertFalse(it.hasNext());
        }
    }
}
