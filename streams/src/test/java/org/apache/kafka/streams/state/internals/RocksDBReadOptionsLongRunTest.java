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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sustained-churn measurement for the {@code defaultReadOptions_} release. Stream time advances
 * continuously so segments roll and expire throughout, which is how the leak accumulates in
 * production. Two things are reported at the end: how many segments still own their native
 * {@code ReadOptions}, which should track the number of segments still open rather than the
 * cumulative number ever opened; and how many distinct native addresses were handed out, which
 * only stays small if the memory is actually returned to the allocator and reused.
 *
 * <p>{@link TopologyTestDriver} rather than a live cluster, because it runs the real store and
 * segment lifecycle single-threaded: the segment map can be sampled safely and stream time can be
 * advanced far enough to churn thousands of segments in seconds.
 *
 * <p>Sized for CI by default. Longer runs via
 * {@code -Dreadoptions.longrun.records=200000}.
 */
public class RocksDBReadOptionsLongRunTest {

    private static final String STORE_NAME = "long-run-counts";
    private static final long WINDOW_SIZE_MS = 1_000L;
    private static final long RETENTION_MS = 120_000L;
    /** Segment interval is max(retention / 2, 60s) = 60s, so this rolls a segment every record. */
    private static final long ADVANCE_MS = 60_000L;

    private static final int RECORDS =
            Integer.parseInt(System.getProperty("readoptions.longrun.records", "3000"));

    private static Field defaultReadOptionsField() throws Exception {
        final Field field = RocksDB.class.getDeclaredField("defaultReadOptions_");
        field.setAccessible(true);
        return field;
    }

    private static Field segmentsField() throws Exception {
        final Field field = AbstractRocksDBSegmentedBytesStore.class.getDeclaredField("segments");
        field.setAccessible(true);
        return field;
    }

    @SuppressWarnings("unchecked")
    private static AbstractSegments<? extends RocksDBStore> segmentsOf(final StateStore store) throws Exception {
        StateStore current = store;
        while (true) {
            if (current instanceof WrappedStateStore) {
                current = (StateStore) ((WrappedStateStore<?, ?, ?>) current).wrapped();
            } else if (current instanceof WindowToTimestampedWindowByteStoreAdapter) {
                // Not a WrappedStateStore, so it needs unwrapping by hand.
                current = ((WindowToTimestampedWindowByteStoreAdapter) current).store;
            } else {
                break;
            }
        }
        if (!(current instanceof AbstractRocksDBSegmentedBytesStore)) {
            throw new AssertionError("expected a segmented RocksDB store, unwrapped to " + current.getClass());
        }
        return (AbstractSegments<? extends RocksDBStore>) segmentsField().get(current);
    }

    @Test
    public void shouldNotAccumulateReadOptionsUnderSustainedSegmentChurn() throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "readoptions-long-run");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_SIZE_MS)))
                .count(Materialized.as(Stores.persistentWindowStore(
                        STORE_NAME,
                        Duration.ofMillis(RETENTION_MS),
                        Duration.ofMillis(WINDOW_SIZE_MS),
                        false)));

        final Field readOptionsField = defaultReadOptionsField();

        // Identity-keyed: one entry per ReadOptions instance ever observed.
        final IdentityHashMap<ReadOptions, Boolean> everSeen = new IdentityHashMap<>();
        final Set<Long> distinctAddresses = new HashSet<>();
        final Field nativeHandle = org.rocksdb.RocksObject.class.getDeclaredField("nativeHandle_");
        nativeHandle.setAccessible(true);

        int maxConcurrentlyOpen = 0;

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(
                    "input", new StringSerializer(), new StringSerializer());

            final AbstractSegments<? extends RocksDBStore> segments =
                    segmentsOf(driver.getAllStateStores().get(STORE_NAME));

            long timestamp = 0L;
            for (int i = 0; i < RECORDS; i++) {
                input.pipeInput("k" + (i % 8), "v" + i, Instant.ofEpochMilli(timestamp));
                timestamp += ADVANCE_MS;

                // Single-threaded driver: safe to walk the segment map directly.
                int open = 0;
                for (final RocksDBStore segment : segments.segments.values()) {
                    if (segment.db == null) {
                        continue;
                    }
                    open++;
                    final ReadOptions ro = (ReadOptions) readOptionsField.get(segment.db);
                    if (ro != null) {
                        everSeen.put(ro, Boolean.TRUE);
                        distinctAddresses.add(nativeHandle.getLong(ro));
                    }
                }
                maxConcurrentlyOpen = Math.max(maxConcurrentlyOpen, open);
            }

            int stillOwning = 0;
            for (final ReadOptions ro : everSeen.keySet()) {
                if (ro.isOwningHandle()) {
                    stillOwning++;
                }
            }

            System.out.printf(
                    "records=%,d  segments observed=%,d  max concurrently open=%d  "
                            + "still owning=%,d  distinct native addresses=%,d%n",
                    RECORDS, everSeen.size(), maxConcurrentlyOpen, stillOwning, distinctAddresses.size());

            // The store is still open, so the currently-live segments legitimately still own their
            // handles. Everything already expired must not.
            assertTrue(stillOwning <= maxConcurrentlyOpen,
                    "still-owning ReadOptions (" + stillOwning + ") should not exceed the number of "
                            + "concurrently open segments (" + maxConcurrentlyOpen + "); with the leak "
                            + "present this grows with the " + everSeen.size() + " segments ever opened");

            // Freed memory gets recycled, so addresses must be reused rather than growing 1:1.
            assertTrue(distinctAddresses.size() < everSeen.size(),
                    "expected native ReadOptions addresses to be reused after release, but saw "
                            + distinctAddresses.size() + " distinct addresses for " + everSeen.size()
                            + " segments — nothing is being returned to the allocator");
        }
    }
}
