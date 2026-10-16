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
import org.apache.kafka.common.utils.Bytes;
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

import java.io.File;
import java.util.Properties;

import static org.apache.kafka.streams.state.internals.RocksDBStore.DB_FILE_DIR;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Close-path leak regression tests for {@link RocksDBMigratingWindowStoreWithHeaders}, paralleling
 * the migrating session-store tests:
 *
 * <ol>
 *   <li>KAFKA-20456 — the offsets-CF {@link ColumnFamilyOptions} must be released on
 *       {@code close()}.</li>
 *   <li>KIP-1035 close path — when the migrating window store opens against pre-existing
 *       default-CF data it installs a {@link DualColumnFamilyAccessor} with three CF handles
 *       (offsets + oldCF + newCF). If the close-time {@code put(closedState)} throws, all three
 *       handles must still be released via the try/finally chain.</li>
 * </ol>
 */
public class RocksDBMigratingWindowStoreWithHeadersCloseLeakTest {
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
        final CapturingOffsetsMigratingWindowStore capturingStore = new CapturingOffsetsMigratingWindowStore();
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
    public void shouldCloseAllDualColumnFamilyHandlesWhenAccessorPutThrowsDuringClose() {
        // Seed the DEFAULT CF so the migrating store enters "upgrade mode" and installs a
        // DualColumnFamilyAccessor rather than the single-CF accessor.
        prepareLegacyDefaultStore();

        rocksDBStore = new RocksDBMigratingWindowStoreWithHeaders(
                DB_NAME, DB_FILE_DIR, new RocksDBMetricsRecorder(METRICS_SCOPE, DB_NAME));
        rocksDBStore.init(context, rocksDBStore);

        final DualColumnFamilyAccessor accessor = (DualColumnFamilyAccessor) rocksDBStore.cfAccessor;
        final ColumnFamilyHandle offsetsHandle = accessor.offsetColumnFamilyHandle();
        final ColumnFamilyHandle oldHandle = accessor.oldColumnFamily();
        final ColumnFamilyHandle newHandle = accessor.newColumnFamily();
        assertTrue(offsetsHandle.isOwningHandle());
        assertTrue(oldHandle.isOwningHandle());
        assertTrue(newHandle.isOwningHandle());

        final ThrowingOnOffsetsPutDBAccessor wrapper =
                new ThrowingOnOffsetsPutDBAccessor(rocksDBStore.dbAccessor, offsetsHandle);
        rocksDBStore.dbAccessor = wrapper;

        rocksDBStore.close();

        assertTrue(wrapper.thrownPutCount.get() >= 1,
                "expected the closedState put on the offsets CF to be invoked and to throw");
        assertFalse(offsetsHandle.isOwningHandle(),
                "offsets CF handle should still be closed when accessor.put throws");
        assertFalse(oldHandle.isOwningHandle(),
                "old CF handle should still be closed when super.close() throws");
        assertFalse(newHandle.isOwningHandle(),
                "new CF handle should still be closed when oldColumnFamily.close() chain runs");
    }

    private void prepareLegacyDefaultStore() {
        final RocksDBStore kvStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            kvStore.init(context, kvStore);
            kvStore.put(new Bytes("seed-key".getBytes()), "seed-value".getBytes());
        } finally {
            kvStore.close();
        }
    }

    private static final class CapturingOffsetsMigratingWindowStore extends RocksDBMigratingWindowStoreWithHeaders {
        ColumnFamilyOptions capturedOffsetsOptions;

        CapturingOffsetsMigratingWindowStore() {
            super(DB_NAME, DB_FILE_DIR, new RocksDBMetricsRecorder(METRICS_SCOPE, DB_NAME));
        }

        @Override
        protected ColumnFamilyOptions offsetsCFOptions() {
            capturedOffsetsOptions = super.offsetsCFOptions();
            return capturedOffsetsOptions;
        }
    }
}
