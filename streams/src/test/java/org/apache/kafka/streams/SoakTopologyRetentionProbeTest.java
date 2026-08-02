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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.WithRetentionPeriod;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.common.utils.Bytes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Root-cause probe for the KIP-892/1035 OOORE investigation.
 *
 * <p>A 20h DEBUG soak showed that KAFKA-13499 / PR #22115's windowed restore
 * optimisation NEVER fires on the soak topology: 0 optimised seeks across ~820
 * no-checkpoint restore decisions, with the stream-stream join stores
 * (JOINTHIS / JOINOTHER / OUTERTHIS / OUTERSHARED / OUTEROTHER) among those
 * failing the gate {@code retentionPeriod > 0 && retentionPeriod != Long.MAX_VALUE}.
 *
 * <p>Probing store shapes by hand against the deployed jar showed every windowed
 * shape PASSING the gate — including the indexed time-ordered join supplier — so
 * construction is not the problem. The remaining hypothesis is that the value the
 * DSL *derives* differs from the value supplied by hand.
 *
 * <p>So this builds the soak's topology verbatim (StreamsSoakTest.java: the five
 * windowed counts at ~L459-517 and the three stream-stream joins at ~L608-625) and
 * dumps, for every store the DSL created, the retention that
 * {@code ProcessorStateManager.extractRetentionPeriod} would resolve.
 *
 * <p>Prints rather than asserts — this is a measurement, not a contract.
 */
public class SoakTopologyRetentionProbeTest {

    /** Verbatim from the soak. */
    private static final JoinWindows JOIN_WINDOWS =
        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(1000), Duration.ofMillis(1000));

    private static final Map<String, String> WINDOWED_CHANGELOG_CONFIGS = new HashMap<>();
    static {
        WINDOWED_CHANGELOG_CONFIGS.put("retention.bytes", "104857600");
        WINDOWED_CHANGELOG_CONFIGS.put("retention.ms", "21600000");
        WINDOWED_CHANGELOG_CONFIGS.put("cleanup.policy", "compact,delete");
    }

    private static Topology soakTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Long> source =
            builder.stream("input", Consumed.with(Serdes.String(), Serdes.Long()));
        final var grouped = source.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

        // --- the five windowed counts, exactly as the soak declares them ---
        grouped.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(10)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.Long())
                .withLoggingEnabled(WINDOWED_CHANGELOG_CONFIGS)
                .withRetention(Duration.ofHours(4)));

        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(45)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.Long())
                .withLoggingEnabled(WINDOWED_CHANGELOG_CONFIGS));

        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2)).advanceBy(Duration.ofSeconds(15)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.Long())
                .withLoggingEnabled(WINDOWED_CHANGELOG_CONFIGS));

        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(5)))
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>with(Serdes.String(), Serdes.Long())
                .withLoggingEnabled(WINDOWED_CHANGELOG_CONFIGS));

        grouped.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(30)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.Long())
                .withLoggingEnabled(WINDOWED_CHANGELOG_CONFIGS));

        // --- a plain (non-windowed) count: the soak has these too, and they
        //     legitimately resolve -1. Included so the output distinguishes them.
        grouped.count(Materialized.with(Serdes.String(), Serdes.Long()));

        // --- the three stream-stream joins, exactly as the soak declares them ---
        final KStream<String, Long> left =
            builder.stream("left", Consumed.with(Serdes.String(), Serdes.Long()));
        final KStream<String, Long> right =
            builder.stream("right", Consumed.with(Serdes.String(), Serdes.Long()));

        left.join(right, (v1, v2) -> "" + v1 + v2, JOIN_WINDOWS,
            StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.Long()));
        left.leftJoin(right, (v1, v2) -> "" + v1 + v2, JOIN_WINDOWS,
            StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.Long()));
        left.outerJoin(right, (v1, v2) -> "" + v1 + v2, JOIN_WINDOWS,
            StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    /** Exactly what ProcessorStateManager.extractRetentionPeriod does. */
    private static long resolveRetention(final StateStore store) {
        StateStore current = store;
        while (current instanceof WrappedStateStore) {
            current = ((WrappedStateStore<?, ?, ?>) current).wrapped();
        }
        return current instanceof WithRetentionPeriod
            ? ((WithRetentionPeriod) current).retentionPeriod() : -1L;
    }

    private static String innermost(final StateStore store) {
        StateStore current = store;
        while (current instanceof WrappedStateStore) {
            current = ((WrappedStateStore<?, ?, ?>) current).wrapped();
        }
        return current.getClass().getSimpleName();
    }

    @Test
    public void dumpResolvedRetentionForEverySoakStore() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "soak-retention-probe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        try (final TopologyTestDriver driver = new TopologyTestDriver(soakTopology(), props)) {
            final Map<String, StateStore> stores = new TreeMap<>(driver.getAllStateStores());
            System.out.printf("%n%-58s %-46s %14s  %s%n",
                "STORE", "INNERMOST", "RETENTION", "#22115 GATE");
            System.out.println("-".repeat(140));
            int pass = 0;
            int fail = 0;
            for (final Map.Entry<String, StateStore> e : stores.entrySet()) {
                final long retention = resolveRetention(e.getValue());
                final boolean gate = retention > 0 && retention != Long.MAX_VALUE;
                if (gate) {
                    pass++;
                } else {
                    fail++;
                }
                System.out.printf("%-58s %-46s %14s  %s%n",
                    e.getKey(), innermost(e.getValue()),
                    retention == Long.MAX_VALUE ? "Long.MAX_VALUE" : String.valueOf(retention),
                    gate ? "PASS (optimised)" : "FAIL (seekToBeginning)");
            }
            System.out.printf("%n  %d stores PASS the gate, %d FAIL%n", pass, fail);
        }
    }
}
