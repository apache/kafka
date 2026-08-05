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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PlainToHeadersWindowStoreAdapter} hides the store it adapts behind a
 * private field instead of participating in the {@link WrappedStateStore} chain.
 * That defeats
 * {@code ProcessorStateManager.StateStoreMetadata.extractRetentionPeriod}, which
 * finds a store's retention by walking to the innermost layer:
 *
 * <pre>
 *   StateStore current = stateStore;
 *   while (current instanceof WrappedStateStore) current = wrapped();
 *   if (current instanceof WithRetentionPeriod) return retentionPeriod();
 *   return -1L;
 * </pre>
 *
 * <p>The walk terminates on the adapter, so the resolved retention is -1 and the
 * KAFKA-13499 / PR #22115 gate
 * {@code retentionPeriod > 0 && retentionPeriod != Long.MAX_VALUE} fails. The
 * windowed restore optimisation then silently falls back to
 * {@code seekToBeginning} for every stream-stream join store, logging nothing
 * above {@code debug}.
 *
 * <p>Observed on the KIP-892/1035 soaks: 0 optimised seeks across ~820
 * no-checkpoint restore decisions in 20 hours.
 */
public class PlainToHeadersAdapterRetentionTest {

    private static final Duration RETENTION = Duration.ofMinutes(45);
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);

    /** Verbatim copy of extractRetentionPeriod's algorithm. */
    private static long resolveRetention(final StateStore store) {
        StateStore current = store;
        while (current instanceof WrappedStateStore) {
            current = ((WrappedStateStore<?, ?, ?>) current).wrapped();
        }
        return current instanceof WithRetentionPeriod
            ? ((WithRetentionPeriod) current).retentionPeriod()
            : -1L;
    }

    private static WindowStore<Bytes, byte[]> plainPersistentWindowStore() {
        // A plain, persistent, non-timestamped window store -- what the adapter
        // is built to wrap, and what a stream-stream join ends up using.
        return Stores.persistentWindowStore("join-store", RETENTION, WINDOW_SIZE, true).get();
    }

    /** Control: without the adapter the retention resolves correctly. */
    @Test
    public void retentionResolvesThroughAnUnadaptedStore() {
        final WindowStore<Bytes, byte[]> store = plainPersistentWindowStore();

        assertEquals(RETENTION.toMillis(), resolveRetention(store),
            "an unadapted persistent window store must expose its retention");
    }

    /**
     * The bug: the same store, wrapped in the adapter, resolves to -1 and so fails
     * the #22115 gate.
     */
    @Test
    public void retentionMustStillResolveThroughThePlainToHeadersAdapter() {
        final StateStore adapted =
            new PlainToHeadersWindowStoreAdapter(plainPersistentWindowStore());

        final long resolved = resolveRetention(adapted);

        assertEquals(RETENTION.toMillis(), resolved,
            "the adapter must not hide the retention of the store it adapts; "
                + "returning -1 silently disables the #22115 windowed restore optimisation");
        assertTrue(resolved > 0 && resolved != Long.MAX_VALUE,
            "the resolved retention must pass the #22115 gate");
    }
}
