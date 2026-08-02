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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.junit.jupiter.api.Test;

import java.time.Duration;

/**
 * Probe for the root question in the KIP-892/1035 OOORE investigation: a DEBUG
 * soak showed that on the soak topology, KAFKA-13499 / PR #22115's windowed
 * restore optimisation NEVER fires — 0 occurrences in 20h across ~820
 * no-checkpoint restore decisions. Every windowed store failed the gate
 * {@code retentionPeriod > 0 && retentionPeriod != Long.MAX_VALUE}.
 *
 * <p>{@code ProcessorStateManager.StateStoreMetadata.extractRetentionPeriod}
 * decides that value by unwrapping the registered store to the innermost layer
 * and checking for {@link WithRetentionPeriod}:
 *
 * <pre>
 *   StateStore current = stateStore;
 *   while (current instanceof WrappedStateStore) current = wrapped();
 *   if (current instanceof WithRetentionPeriod) return retentionPeriod();
 *   return -1L;
 * </pre>
 *
 * Only four classes implement {@code WithRetentionPeriod}:
 * {@code AbstractRocksDBSegmentedBytesStore},
 * {@code AbstractDualSchemaRocksDBSegmentedBytesStore},
 * {@code InMemoryWindowStore}, {@code InMemorySessionStore}.
 *
 * <p>This test does not assert; it PRINTS the unwrap chain for each store shape
 * the soak builds, so we can see exactly where the walk terminates and what the
 * resolved retention is. Run with:
 *
 * <pre>
 *   ./gradlew :streams:test --tests '*RetentionPeriodUnwrapProbeTest' \
 *       --console=plain -i
 * </pre>
 */
public class RetentionPeriodUnwrapProbeTest {

    private static final Duration RETENTION = Duration.ofMinutes(45);
    private static final Duration WINDOW = Duration.ofSeconds(1);

    /** Replicates extractRetentionPeriod, but reports the whole chain. */
    private static void probe(final String label, final StateStore store) {
        final StringBuilder chain = new StringBuilder();
        StateStore current = store;
        chain.append(current.getClass().getSimpleName());
        while (current instanceof WrappedStateStore) {
            current = ((WrappedStateStore<?, ?, ?>) current).wrapped();
            chain.append("\n        -> ").append(current.getClass().getSimpleName());
        }
        final boolean has = current instanceof WithRetentionPeriod;
        final long resolved = has ? ((WithRetentionPeriod) current).retentionPeriod() : -1L;
        final boolean gatePasses = resolved > 0 && resolved != Long.MAX_VALUE;

        System.out.printf(
            "%n=== %s%n    chain: %s%n    innermost implements WithRetentionPeriod: %s%n"
                + "    resolved retentionPeriod: %s%n    #22115 gate passes: %s%n",
            label, chain, has, resolved, gatePasses ? "YES (optimised)" : "NO -> seekToBeginning");
    }

    @Test
    public void probeTheStoreShapesTheSoakBuilds() {
        // 1. windowed aggregation, what the DSL builds for
        //    windowedBy(TimeWindows).count(Materialized...) -> TIMESTAMPED window store
        probe("timestamped window store (windowed count / AGGREGATE-STATE-STORE)",
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore("agg", RETENTION, WINDOW, false),
                Serdes.String(), Serdes.Long()).build());

        // 2. stream-stream join window store: retainDuplicates = true
        probe("window store, retainDuplicates=true (JOINTHIS / OUTEROTHER)",
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore("join", RETENTION, WINDOW, true),
                Serdes.String(), Serdes.Long()).build());

        // 3. plain (non-timestamped) window store
        probe("window store, retainDuplicates=false",
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore("win", RETENTION, WINDOW, false),
                Serdes.String(), Serdes.Long()).build());

        // 4. session store
        probe("session store (SessionWindows count)",
            Stores.sessionStoreBuilder(
                Stores.persistentSessionStore("sess", RETENTION),
                Serdes.String(), Serdes.Long()).build());

        // 5. in-memory window store -- one of the four implementors, as a control
        probe("IN-MEMORY window store (control: should pass the gate)",
            Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore("mem", RETENTION, WINDOW, false),
                Serdes.String(), Serdes.Long()).build());

        // 6. plain key-value: legitimately -1
        final StoreBuilder<?> kv = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("kv"), Serdes.String(), Serdes.Long());
        probe("key-value store (control: legitimately -1)", kv.build());
    }
}
