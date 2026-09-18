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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
