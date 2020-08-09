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
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSet;
import static org.junit.Assert.assertEquals;


public class RocksDBSessionStoreTest extends AbstractSessionBytesStoreTest {

    private static final String STORE_NAME = "rocksDB session store";

    @Override
    <K, V> SessionStore<K, V> buildSessionStore(final long retentionPeriod,
                                                 final Serde<K> keySerde,
                                                 final Serde<V> valueSerde) {
        return Stores.sessionStoreBuilder(
            Stores.persistentSessionStore(
                STORE_NAME,
                ofMillis(retentionPeriod)),
            keySerde,
            valueSerde).build();
    }

    @Override
    String getMetricsScope() {
        return new RocksDbSessionBytesStoreSupplier(null, 0).metricsScope();
    }

    @Test
    public void shouldRemoveExpired() {
        sessionStore.put(new Windowed<>("a", Window.withBounds(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", Window.withBounds(0, SEGMENT_INTERVAL)), 2L);
        sessionStore.put(new Windowed<>("a", Window.withBounds(10, SEGMENT_INTERVAL)), 3L);

        // Advance stream time to expire the first record
        sessionStore.put(new Windowed<>("aa", Window.withBounds(10, 2 * SEGMENT_INTERVAL)), 4L);

        try (final KeyValueIterator<Windowed<String>, Long> iterator =
            sessionStore.findSessions("a", "b", 0L, Long.MAX_VALUE)
        ) {
            assertEquals(valuesToSet(iterator), new HashSet<>(Arrays.asList(2L, 3L, 4L)));
        }
    }
}