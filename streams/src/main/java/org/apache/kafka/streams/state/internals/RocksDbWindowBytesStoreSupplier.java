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
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class RocksDbWindowBytesStoreSupplier implements WindowBytesStoreSupplier {
    private final String name;
    private final long retentionPeriod;
    private final long segmentInterval;
    private final long windowSize;
    private final boolean retainDuplicates;

    public RocksDbWindowBytesStoreSupplier(final String name,
                                           final long retentionPeriod,
                                           final long segmentInterval,
                                           final long windowSize,
                                           final boolean retainDuplicates) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.segmentInterval = segmentInterval;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        final RocksDBSegmentedBytesStore segmentedBytesStore = new RocksDBSegmentedBytesStore(
                name,
                metricsScope(),
                retentionPeriod,
                segmentInterval,
                new WindowKeySchema()
        );
        return new RocksDBWindowStore<>(segmentedBytesStore,
                Serdes.Bytes(),
                Serdes.ByteArray(),
                retainDuplicates,
                windowSize);

    }

    @Override
    public String metricsScope() {
        return "rocksdb-window-state";
    }

    @Deprecated
    @Override
    public int segments() {
        return (int) (retentionPeriod / segmentInterval) + 1;
    }

    @Override
    public long segmentIntervalMs() {
        return segmentInterval;
    }

    @Override
    public long windowSize() {
        return windowSize;
    }

    @Override
    public boolean retainDuplicates() {
        return retainDuplicates;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }
}
