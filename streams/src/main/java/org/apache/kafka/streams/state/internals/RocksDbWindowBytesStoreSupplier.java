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
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class RocksDbWindowBytesStoreSupplier implements WindowBytesStoreSupplier {
    private final String name;
    private final long retentionPeriod;
    private final int segments;
    private final long windowSize;
    private final boolean retainDuplicates;

    @SuppressWarnings("deprecation")
    public RocksDbWindowBytesStoreSupplier(final String name,
                                           final long retentionPeriod,
                                           final int segments,
                                           final long windowSize,
                                           final boolean retainDuplicates) {
        if (segments < org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier.MIN_SEGMENTS) {
            throw new IllegalArgumentException("numSegments must be >= " + org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier.MIN_SEGMENTS);
        }
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.segments = segments;
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
                retentionPeriod,
                segments,
                new WindowKeySchema()
        );
        return RocksDBWindowStore.bytesStore(segmentedBytesStore,
                                             retainDuplicates,
                                             windowSize);

    }

    @Override
    public String metricsScope() {
        return "rocksdb-window";
    }

    @Override
    public int segments() {
        return segments;
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
