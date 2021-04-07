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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;

import static org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier.WindowStoreTypes;

public class RocksDBTimeOrderedWindowStoreTest extends RocksDBWindowStoreTest {
    private static final String STORE_NAME = "rocksDB window store";

    @Override
    <K, V> WindowStore<K, V> buildWindowStore(final long retentionPeriod,
                                              final long windowSize,
                                              final boolean retainDuplicates,
                                              final Serde<K> keySerde,
                                              final Serde<V> valueSerde) {
        return new TimeOrderedWindowStoreBuilder<>(
            new RocksDbWindowBytesStoreSupplier(
                STORE_NAME,
                retentionPeriod,
                Math.max(retentionPeriod / 2, 60_000L),
                windowSize,
                retainDuplicates,
                WindowStoreTypes.TIME_ORDERED_WINDOW_STORE),
            keySerde,
            valueSerde,
            Time.SYSTEM)
            .build();
    }

    @Override
    String getMetricsScope() {
        return new RocksDbWindowBytesStoreSupplier(null, 0, 0, 0, false, WindowStoreTypes.TIME_ORDERED_WINDOW_STORE).metricsScope();
    }

    @Override
    void setClassLoggerToDebug() {
        LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore.class);
    }

    @Override
    long extractStoreTimestamp(final byte[] binaryKey) {
        return TimeOrderedKeySchema.extractStoreTimestamp(binaryKey);
    }

    @Override
    <K> K extractStoreKey(final byte[] binaryKey,
                          final StateSerdes<K, ?> serdes) {
        return TimeOrderedKeySchema.extractStoreKey(binaryKey, serdes);
    }
}
