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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for two RocksDBStore close-path native-memory leaks:
 *
 * <ol>
 * <li>KAFKA-20456 — the {@link ColumnFamilyOptions} returned by
 *     {@code RocksDBStore#offsetsCFOptions()} must be released on {@code close()}.
 *     Constructing a {@code ColumnFamilyOptions} on the JNI side auto-allocates a default
 *     {@code BlockBasedTableFactory} and its {@code LRUCache}; the cache itself has no Java
 *     handle, so we assert on the options' own handle and rely on the destructor contract.</li>
 * <li>KIP-1035 close-path — for non-transactional stores, {@link AbstractColumnFamilyAccessor#close}
 *     writes a closed-state marker to the offsets CF. If that {@code put} throws (e.g. during
 *     an EOSv2 fencing cascade or unclean shutdown), the column family handles must still be
 *     released. We simulate the throw via {@link ThrowingOnOffsetsPutDBAccessor} and observe via
 *     {@code isOwningHandle()} because {@code RocksDBStore.close()} swallows the resulting
 *     {@code RocksDBException}.</li>
 * <li>{@code defaultReadOptions_} — {@code RocksDB} allocates a {@link ReadOptions} per instance
 *     and never closes it, so {@code RocksDBStore} must. Covers the normal close, the
 *     {@code openDB} error-cleanup path, expiry-driven segment close, and the versioned-store
 *     shape where many logical segments share one physical database.</li>
 * </ol>
 */
public class RocksDBStoreCloseLeakTest {
    private static final String DB_NAME = "db-name";
    private static final String METRICS_SCOPE = "metrics-scope";

    private InternalMockProcessorContext<?, ?> context;
    private RocksDBStore rocksDBStore;

    @BeforeEach
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        final File dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
    }

    @AfterEach
    public void tearDown() {
        if (rocksDBStore != null) {
            rocksDBStore.close();
        }
    }

    @Test
    public void shouldCloseOffsetsCfOptionsOnStoreClose() {
        final CapturingOffsetsRocksDBStore capturingStore = new CapturingOffsetsRocksDBStore();
        rocksDBStore = capturingStore;
        rocksDBStore.init(context, rocksDBStore);

        final ColumnFamilyOptions captured = capturingStore.capturedOffsetsOptions;
        assertNotNull(captured, "offsetsCFOptions should have been invoked during init");
        assertTrue(captured.isOwningHandle(),
                "offsets CF options should own its native handle while store is open");

        rocksDBStore.close();

        assertFalse(captured.isOwningHandle(),
                "offsets CF options native handle should be released by close()");
    }

    @Test
    public void shouldCloseColumnFamilyHandlesWhenAccessorPutThrowsDuringClose() {
        rocksDBStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        rocksDBStore.init(context, rocksDBStore);

        final RocksDBStore.SingleColumnFamilyAccessor accessor =
                (RocksDBStore.SingleColumnFamilyAccessor) rocksDBStore.cfAccessor;
        final ColumnFamilyHandle offsetsHandle = accessor.offsetColumnFamilyHandle();
        final ColumnFamilyHandle dataHandle = accessor.columnFamily();
        assertTrue(offsetsHandle.isOwningHandle());
        assertTrue(dataHandle.isOwningHandle());

        final ThrowingOnOffsetsPutDBAccessor wrapper =
                new ThrowingOnOffsetsPutDBAccessor(rocksDBStore.dbAccessor, offsetsHandle);
        rocksDBStore.dbAccessor = wrapper;

        rocksDBStore.close();

        assertTrue(wrapper.thrownPutCount.get() >= 1,
                "expected the closedState put on the offsets CF to be invoked and to throw");
        assertFalse(offsetsHandle.isOwningHandle(),
                "offsets CF handle should still be closed when accessor.put throws");
        assertFalse(dataHandle.isOwningHandle(),
                "data CF handle should still be closed when super.close() throws");
    }

    @Test
    public void shouldReleaseDefaultReadOptionsOnStoreClose() throws Exception {
        rocksDBStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        rocksDBStore.init(context, rocksDBStore);

        // Capture before close(): RocksDBStore nulls out `db` as part of closing.
        final ReadOptions defaultReadOptions = defaultReadOptionsOf(rocksDBStore.db);
        assertTrue(defaultReadOptions.isOwningHandle(),
                "defaultReadOptions_ should own its native handle while the store is open");

        rocksDBStore.close();

        assertFalse(defaultReadOptions.isOwningHandle(),
                "RocksDB.close() never releases defaultReadOptions_, so RocksDBStore must do it");
    }

    @Test
    public void shouldReleaseDefaultReadOptionsWhenOpenFailsPartway() {
        final FailAfterOpenRocksDBStore store = new FailAfterOpenRocksDBStore();
        rocksDBStore = store;

        assertThrows(RuntimeException.class, () -> store.init(context, store));

        assertNotNull(store.capturedReadOptions, "the database should have been opened before the failure");
        assertFalse(store.capturedReadOptions.isOwningHandle(),
                "the openDB error-cleanup path must release defaultReadOptions_ as well");
    }

    @Test
    public void shouldNotThrowWhenDefaultReadOptionsReleasedTwice() {
        rocksDBStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        rocksDBStore.init(context, rocksDBStore);
        final RocksDB db = rocksDBStore.db;

        rocksDBStore.close();

        // close() already released it; a second release must be a no-op rather than a crash.
        RocksDBStore.releaseDefaultReadOptions(db);
    }

    // Segments are closed by cleanupExpiredSegments during ordinary processing, not only at
    // shutdown, and each segment is its own RocksDB: this is where the leak accumulates fastest.
    @Test
    public void shouldReleaseDefaultReadOptionsWhenSegmentExpires() throws Exception {
        final long retentionPeriod = 100L;
        final long segmentInterval = 10L;
        final KeyValueSegments segments =
                new KeyValueSegments("test-segments", METRICS_SCOPE, retentionPeriod, segmentInterval);
        try {
            segments.openExisting(context, 0L);

            final KeyValueSegment expiringSegment = segments.getOrCreateSegmentIfLive(0, context, 0L);
            assertNotNull(expiringSegment, "segment 0 should be live at stream time 0");
            final ReadOptions segmentReadOptions = defaultReadOptionsOf(expiringSegment.db);
            assertTrue(segmentReadOptions.isOwningHandle());

            // Advance stream time past segment 0's retention. minLiveSegment becomes
            // (200 - 100) / 10 = 10, so segment 0 is expired and closed in-flight.
            assertNotNull(segments.getOrCreateSegmentIfLive(20, context, 200L));

            assertFalse(segmentReadOptions.isOwningHandle(),
                    "expiry-driven segment close must release defaultReadOptions_");
        } finally {
            segments.close();
        }
    }

    // Versioned stores share one physical RocksDB across all logical segments, so releasing on
    // logical-segment expiry would free the handle out from under the other segments.
    @Test
    public void shouldReleasePhysicalStoreReadOptionsOnlyWhenTheWholeStoreCloses() throws Exception {
        final LogicalKeyValueSegments segments = new LogicalKeyValueSegments(
                "logical-segments",
                RocksDBStore.DB_FILE_DIR,
                100L,
                10L,
                new RocksDBMetricsRecorder(METRICS_SCOPE, "logical-segments"));
        final ReadOptions physicalReadOptions;
        try {
            segments.openExisting(context, 0L);
            physicalReadOptions = defaultReadOptionsOf(segments.physicalStore().db);
            assertTrue(physicalReadOptions.isOwningHandle());

            assertNotNull(segments.getOrCreateSegmentIfLive(0, context, 0L));
            // Expires logical segment 0 while the shared physical store stays open.
            assertNotNull(segments.getOrCreateSegmentIfLive(20, context, 200L));

            assertTrue(physicalReadOptions.isOwningHandle(),
                    "expiring a logical segment must NOT release the shared physical store's "
                            + "defaultReadOptions_ — other logical segments are still using that database");
        } finally {
            segments.close();
        }

        assertFalse(physicalReadOptions.isOwningHandle(),
                "closing the segments should release the physical store's defaultReadOptions_");
    }

    // Fails right after the database is open, so openDB's error-cleanup path runs with a live
    // RocksDB to release.
    private static final class FailAfterOpenRocksDBStore extends RocksDBStore {
        ReadOptions capturedReadOptions;

        FailAfterOpenRocksDBStore() {
            super(DB_NAME, METRICS_SCOPE);
        }

        @Override
        void openRocksDB(final DBOptions dbOptions, final ColumnFamilyOptions columnFamilyOptions) {
            super.openRocksDB(dbOptions, columnFamilyOptions);
            try {
                capturedReadOptions = defaultReadOptionsOf(db);
            } catch (final Exception e) {
                throw new AssertionError("could not read defaultReadOptions_", e);
            }
            throw new IllegalStateException("simulated failure after the database was opened");
        }
    }

    // The field is private with no accessor, so reflection is the only way to observe it.
    private static ReadOptions defaultReadOptionsOf(final RocksDB db) throws Exception {
        final Field field = RocksDB.class.getDeclaredField("defaultReadOptions_");
        field.setAccessible(true);
        return (ReadOptions) field.get(db);
    }

    private static final class CapturingOffsetsRocksDBStore extends RocksDBStore {
        ColumnFamilyOptions capturedOffsetsOptions;

        CapturingOffsetsRocksDBStore() {
            super(DB_NAME, METRICS_SCOPE);
        }

        @Override
        protected ColumnFamilyOptions offsetsCFOptions() {
            capturedOffsetsOptions = super.offsetsCFOptions();
            return capturedOffsetsOptions;
        }
    }
}
