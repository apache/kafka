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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.StreamsTestUtils.valuesToSetAndCloseIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public abstract class AbstractRocksDBWindowStoreTest extends AbstractWindowBytesStoreTest {

    private static final String STORE_NAME = "rocksDB window store";
    private static final String METRICS_SCOPE = "test-state-id";

    private final KeyValueSegments segments =
            new KeyValueSegments(STORE_NAME, METRICS_SCOPE, RETENTION_PERIOD, SEGMENT_INTERVAL);

    enum StoreType {
        RocksDBWindowStore,
        RocksDBTimeOrderedWindowStoreWithIndex,
        RocksDBTimeOrderedWindowStoreWithoutIndex
    }

    abstract StoreType storeType();

    @Override
    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                              final long windowSize,
                                              final boolean retainDuplicates,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {

        switch (storeType()) {
            case RocksDBWindowStore: {
                return Stores.windowStoreBuilder(
                                Stores.persistentWindowStore(
                                        STORE_NAME,
                                        ofMillis(retentionPeriod),
                                        ofMillis(windowSize),
                                        retainDuplicates),
                                keySerde,
                                valueSerde)
                        .build();
            }
            case RocksDBTimeOrderedWindowStoreWithIndex: {
                final long defaultSegmentInterval = Math.max(retentionPeriod / 2, 60_000L);
                return Stores.windowStoreBuilder(
                        new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(STORE_NAME,
                                retentionPeriod, defaultSegmentInterval, windowSize, retainDuplicates,
                                true),
                        keySerde,
                        valueSerde
                ).build();
            }
            case RocksDBTimeOrderedWindowStoreWithoutIndex: {
                final long defaultSegmentInterval = Math.max(retentionPeriod / 2, 60_000L);
                return Stores.windowStoreBuilder(
                        new RocksDbIndexedTimeOrderedWindowBytesStoreSupplier(STORE_NAME,
                                retentionPeriod, defaultSegmentInterval, windowSize, retainDuplicates,
                                false),
                        keySerde,
                        valueSerde
                ).build();
            }
            default:
                throw new IllegalStateException("Unknown StoreType: " + storeType());
        }
    }

    @Test
    public void shouldOnlyIterateOpenSegments() {
        long currentTime = 0;
        windowStore.put(1, "one", currentTime);

        currentTime = currentTime + SEGMENT_INTERVAL;
        windowStore.put(1, "two", currentTime);
        currentTime = currentTime + SEGMENT_INTERVAL;

        windowStore.put(1, "three", currentTime);

        try (final WindowStoreIterator<String> iterator = windowStore.fetch(1, 0L, currentTime)) {

            // roll to the next segment that will close the first
            currentTime = currentTime + SEGMENT_INTERVAL;
            windowStore.put(1, "four", currentTime);

            // should only have 2 values as the first segment is no longer open
            assertEquals(new KeyValue<>(SEGMENT_INTERVAL, "two"), iterator.next());
            assertEquals(new KeyValue<>(2 * SEGMENT_INTERVAL, "three"), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testRolling() {
        // to validate segments
        final long startTime = SEGMENT_INTERVAL * 2;
        final long increment = SEGMENT_INTERVAL / 2;
        windowStore.put(0, "zero", startTime);
        assertEquals(Set.of(segments.segmentName(2)), segmentDirs(baseDir));

        windowStore.put(1, "one", startTime + increment);
        assertEquals(Set.of(segments.segmentName(2)), segmentDirs(baseDir));

        windowStore.put(2, "two", startTime + increment * 2);
        assertEquals(
                Set.of(
                        segments.segmentName(2),
                        segments.segmentName(3)
                ),
                segmentDirs(baseDir)
        );

        windowStore.put(4, "four", startTime + increment * 4);
        assertEquals(
                Set.of(
                        segments.segmentName(2),
                        segments.segmentName(3),
                        segments.segmentName(4)
                ),
                segmentDirs(baseDir)
        );

        windowStore.put(5, "five", startTime + increment * 5);
        assertEquals(
                Set.of(
                        segments.segmentName(2),
                        segments.segmentName(3),
                        segments.segmentName(4)
                ),
                segmentDirs(baseDir)
        );
        // For all tests, for WindowStore actualFrom is computed using observedStreamTime - retention + 1.
        // while for TimeOrderedWindowStores, actualFrom = observedStreamTime - retention
        // expired record
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        // RocksDbWindwStore =>
        //  from = 149997
        //  to = 150003
        //  actualFrom = 150001
        // record one timestamp is 150,000 So, it's ignored.
        // RocksDBTimeOrderedWindowStore*Index =>
        //  from = 149997
        //  to = 150003
        //  actualFrom = 150000, hence not ignored
        if (storeType() == StoreType.RocksDBWindowStore) {
            assertEquals(
                    new HashSet<>(Collections.emptyList()),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            1,
                            ofEpochMilli(startTime + increment - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment + WINDOW_SIZE))));

        } else {
            assertEquals(
                    new HashSet<>(Collections.singletonList("one")),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            1,
                            ofEpochMilli(startTime + increment - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        }
        assertEquals(
                new HashSet<>(Collections.singletonList("two")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        2,
                        ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("four")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        4,
                        ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("five")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));

        windowStore.put(6, "six", startTime + increment * 6);
        assertEquals(
                Set.of(
                        segments.segmentName(3),
                        segments.segmentName(4),
                        segments.segmentName(5)
                ),
                segmentDirs(baseDir)
        );

        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        1,
                        ofEpochMilli(startTime + increment - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        // RocksDbWindwStore =>
        //  from = 179997
        //  to = 180003
        //  actualFrom = 170001
        // record one timestamp is 180,000 So, it's ignored.
        // RocksDBTimeOrderedWindowStore*Index =>
        //  from = 179997
        //  to = 180003
        //  actualFrom = 180000, hence not ignored
        if (storeType() == StoreType.RocksDBWindowStore) {
            assertEquals(
                    // expired record
                    new HashSet<>(Collections.emptyList()),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            2,
                            ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        } else {
            assertEquals(
                    // expired record
                    new HashSet<>(Collections.singletonList("two")),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            2,
                            ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        }
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("four")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        4,
                        ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("five")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("six")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        6,
                        ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));

        windowStore.put(7, "seven", startTime + increment * 7);
        assertEquals(
                Set.of(
                        segments.segmentName(3),
                        segments.segmentName(4),
                        segments.segmentName(5)
                ),
                segmentDirs(baseDir)
        );

        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        1,
                        ofEpochMilli(startTime + increment - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        assertEquals(
                // expired record
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        2,
                        ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("four")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        4,
                        ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("five")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("six")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        6,
                        ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("seven")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        7,
                        ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));

        windowStore.put(8, "eight", startTime + increment * 8);
        assertEquals(
                Set.of(
                        segments.segmentName(4),
                        segments.segmentName(5),
                        segments.segmentName(6)
                ),
                segmentDirs(baseDir)
        );

        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        1,
                        ofEpochMilli(startTime + increment - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        2,
                        ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        if (storeType() == StoreType.RocksDBWindowStore) {
            assertEquals(
                    // expired record
                    new HashSet<>(Collections.emptyList()),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            4,
                            ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        } else {
            assertEquals(
                    // expired record
                    new HashSet<>(Collections.singletonList("four")),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            4,
                            ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));

        }
        assertEquals(
                new HashSet<>(Collections.singletonList("five")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("six")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        6,
                        ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("seven")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        7,
                        ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("eight")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        8,
                        ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

        // check segment directories
        windowStore.flush();
        assertEquals(
                Set.of(
                        segments.segmentName(4),
                        segments.segmentName(5),
                        segments.segmentName(6)
                ),
                segmentDirs(baseDir)
        );
    }

    @Test
    public void testSegmentMaintenance() {
        windowStore.close();
        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, true, Serdes.Integer(),
                Serdes.String());
        windowStore.init(context, windowStore);

        context.setTime(0L);
        windowStore.put(0, "v", 0);
        assertEquals(
                Set.of(segments.segmentName(0L)),
                segmentDirs(baseDir)
        );

        windowStore.put(0, "v", SEGMENT_INTERVAL - 1);
        windowStore.put(0, "v", SEGMENT_INTERVAL - 1);
        assertEquals(
                Set.of(segments.segmentName(0L)),
                segmentDirs(baseDir)
        );

        windowStore.put(0, "v", SEGMENT_INTERVAL);
        assertEquals(
                Set.of(segments.segmentName(0L), segments.segmentName(1L)),
                segmentDirs(baseDir)
        );

        int fetchedCount;

        try (final WindowStoreIterator<String> iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(SEGMENT_INTERVAL * 4))) {
            fetchedCount = 0;
            while (iter.hasNext()) {
                iter.next();
                fetchedCount++;
            }
        }
        assertEquals(4, fetchedCount);

        assertEquals(
                Set.of(segments.segmentName(0L), segments.segmentName(1L)),
                segmentDirs(baseDir)
        );

        windowStore.put(0, "v", SEGMENT_INTERVAL * 3);

        try (final WindowStoreIterator<String> iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(SEGMENT_INTERVAL * 4))) {
            fetchedCount = 0;
            while (iter.hasNext()) {
                iter.next();
                fetchedCount++;
            }
        }
        // 1 extra record is expired in the case of RocksDBWindowStore as
        // actualFrom = observedStreamTime - retentionPeriod + 1. The +1
        // isn't present for RocksDbTimeOrderedStoreWith*Index
        if (storeType() == StoreType.RocksDBWindowStore) {
            assertEquals(1, fetchedCount);
        } else {
            assertEquals(2, fetchedCount);
        }

        assertEquals(
                Set.of(segments.segmentName(1L), segments.segmentName(3L)),
                segmentDirs(baseDir)
        );

        windowStore.put(0, "v", SEGMENT_INTERVAL * 5);

        try (final WindowStoreIterator<String> iter = windowStore.fetch(0, ofEpochMilli(SEGMENT_INTERVAL * 4), ofEpochMilli(SEGMENT_INTERVAL * 10))) {
            fetchedCount = 0;
            while (iter.hasNext()) {
                iter.next();
                fetchedCount++;
            }
        }

        // the latest record has a timestamp > 60k. So, the +1 in actualFrom calculation in
        // RocksDbWindowStore shouldn't have an implciation and all stores should return the same fetched counts.
        assertEquals(1, fetchedCount);
        assertEquals(
                Set.of(segments.segmentName(3L), segments.segmentName(5L)),
                segmentDirs(baseDir)
        );

    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testInitialLoading() {
        final File storeDir = new File(baseDir, STORE_NAME);

        new File(storeDir, segments.segmentName(0L)).mkdir();
        new File(storeDir, segments.segmentName(1L)).mkdir();
        new File(storeDir, segments.segmentName(2L)).mkdir();
        new File(storeDir, segments.segmentName(3L)).mkdir();
        new File(storeDir, segments.segmentName(4L)).mkdir();
        new File(storeDir, segments.segmentName(5L)).mkdir();
        new File(storeDir, segments.segmentName(6L)).mkdir();
        windowStore.close();

        windowStore = buildWindowStore(RETENTION_PERIOD, WINDOW_SIZE, false, Serdes.Integer(), Serdes.String());
        windowStore.init(context, windowStore);

        // put something in the store to advance its stream time and expire the old segments
        windowStore.put(1, "v", 6L * SEGMENT_INTERVAL);

        final List<String> expected = asList(
                segments.segmentName(4L),
                segments.segmentName(5L),
                segments.segmentName(6L));
        expected.sort(String::compareTo);

        final List<String> actual = Utils.toList(segmentDirs(baseDir).iterator());
        actual.sort(String::compareTo);

        assertEquals(expected, actual);

        try (final WindowStoreIterator<String> iter = windowStore.fetch(0, ofEpochMilli(0L), ofEpochMilli(1000000L))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }

        assertEquals(
                Set.of(
                        segments.segmentName(4L),
                        segments.segmentName(5L),
                        segments.segmentName(6L)),
                segmentDirs(baseDir)
        );
    }

    @Test
    public void testRestore() throws Exception {
        final long startTime = SEGMENT_INTERVAL * 2;
        final long increment = SEGMENT_INTERVAL / 2;

        windowStore.put(0, "zero", startTime);
        windowStore.put(1, "one", startTime + increment);
        windowStore.put(2, "two", startTime + increment * 2);
        windowStore.put(3, "three", startTime + increment * 3);
        windowStore.put(4, "four", startTime + increment * 4);
        windowStore.put(5, "five", startTime + increment * 5);
        windowStore.put(6, "six", startTime + increment * 6);
        windowStore.put(7, "seven", startTime + increment * 7);
        windowStore.put(8, "eight", startTime + increment * 8);
        windowStore.flush();

        windowStore.close();

        // remove local store image
        Utils.delete(baseDir);

        windowStore = buildWindowStore(RETENTION_PERIOD,
                WINDOW_SIZE,
                false,
                Serdes.Integer(),
                Serdes.String());
        windowStore.init(context, windowStore);

        // For all tests, for WindowStore actualFrom is computed using observedStreamTime - retention + 1.
        // while for TimeOrderedWindowStores, actualFrom = observedStreamTime - retention

        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        1,
                        ofEpochMilli(startTime + increment - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        2,
                        ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        4,
                        ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        6,
                        ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        7,
                        ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        8,
                        ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

        final List<KeyValue<byte[], byte[]>> changeLog = new ArrayList<>();
        for (final ProducerRecord<Object, Object> record : recordCollector.collected()) {
            changeLog.add(new KeyValue<>(((Bytes) record.key()).get(), (byte[]) record.value()));
        }

        context.restore(STORE_NAME, changeLog);

        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        0,
                        ofEpochMilli(startTime - WINDOW_SIZE),
                        ofEpochMilli(startTime + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        1,
                        ofEpochMilli(startTime + increment - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        2,
                        ofEpochMilli(startTime + increment * 2 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 2 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.emptyList()),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        3,
                        ofEpochMilli(startTime + increment * 3 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 3 + WINDOW_SIZE))));
        // RocksDbWindwStore =>
        //  from = 239,997
        //  to = 240,003
        //  actualFrom = 240,001
        // record four timestamp is 240,000 So, it's ignored.
        // RocksDBTimeOrderedWindowStore*Index =>
        //  from = 239,997
        //  to = 240,003
        //  actualFrom = 240,000, hence not ignored
        if (storeType() == StoreType.RocksDBWindowStore) {
            assertEquals(
                    new HashSet<>(Collections.emptyList()),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            4,
                            ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));
        } else {
            assertEquals(
                    new HashSet<>(Collections.singletonList("four")),
                    valuesToSetAndCloseIterator(windowStore.fetch(
                            4,
                            ofEpochMilli(startTime + increment * 4 - WINDOW_SIZE),
                            ofEpochMilli(startTime + increment * 4 + WINDOW_SIZE))));

        }
        assertEquals(
                new HashSet<>(Collections.singletonList("five")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        5,
                        ofEpochMilli(startTime + increment * 5 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 5 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("six")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        6,
                        ofEpochMilli(startTime + increment * 6 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 6 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("seven")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        7,
                        ofEpochMilli(startTime + increment * 7 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 7 + WINDOW_SIZE))));
        assertEquals(
                new HashSet<>(Collections.singletonList("eight")),
                valuesToSetAndCloseIterator(windowStore.fetch(
                        8,
                        ofEpochMilli(startTime + increment * 8 - WINDOW_SIZE),
                        ofEpochMilli(startTime + increment * 8 + WINDOW_SIZE))));

        // check segment directories
        windowStore.flush();
        assertEquals(
                Set.of(
                        segments.segmentName(4L),
                        segments.segmentName(5L),
                        segments.segmentName(6L)),
                segmentDirs(baseDir)
        );
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldMatchPositionAfterPut() {
        final MeteredWindowStore<Integer, String> meteredSessionStore = (MeteredWindowStore<Integer, String>) windowStore;
        final ChangeLoggingWindowBytesStore changeLoggingSessionBytesStore = (ChangeLoggingWindowBytesStore) meteredSessionStore.wrapped();
        final WrappedStateStore<WrappedStateStore, Integer, String> rocksDBWindowStore = (WrappedStateStore<WrappedStateStore, Integer, String>) changeLoggingSessionBytesStore.wrapped();

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        windowStore.put(0, "0", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        windowStore.put(1, "1", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        windowStore.put(2, "2", SEGMENT_INTERVAL);
        context.setRecordContext(new ProcessorRecordContext(0, 4, 0, "", new RecordHeaders()));
        windowStore.put(3, "3", SEGMENT_INTERVAL);

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 4L)))));
        final Position actual = rocksDBWindowStore.getPosition();
        assertEquals(expected, actual);
    }

    private Set<String> segmentDirs(final File baseDir) {
        final File windowDir = new File(baseDir, windowStore.name());

        return new HashSet<>(asList(requireNonNull(windowDir.list())));
    }

}