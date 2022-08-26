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
    public enum WindowStoreTypes {
        DEFAULT_WINDOW_STORE,
        TIMESTAMPED_WINDOW_STORE
    }

    private final String name;
    private final long retentionPeriod;
    private final long segmentInterval;
    private final long windowSize;
    private final boolean retainDuplicates;
    private final WindowStoreTypes windowStoreType;

    public RocksDbWindowBytesStoreSupplier(final String name,
                                           final long retentionPeriod,
                                           final long segmentInterval,
                                           final long windowSize,
                                           final boolean retainDuplicates,
                                           final boolean returnTimestampedStore) {
        this(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates,
            returnTimestampedStore
                ? WindowStoreTypes.TIMESTAMPED_WINDOW_STORE
                : WindowStoreTypes.DEFAULT_WINDOW_STORE);
    }

    public RocksDbWindowBytesStoreSupplier(final String name,
                                           final long retentionPeriod,
                                           final long segmentInterval,
                                           final long windowSize,
                                           final boolean retainDuplicates,
                                           final WindowStoreTypes windowStoreType) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.segmentInterval = segmentInterval;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.windowStoreType = windowStoreType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        switch (windowStoreType) {
            case DEFAULT_WINDOW_STORE:
                return new RocksDBWindowStore(
                    new RocksDBSegmentedBytesStore(
                        name,
                        metricsScope(),
                        retentionPeriod,
                        segmentInterval,
                        new WindowKeySchema()),
                    retainDuplicates,
                    windowSize);
            case TIMESTAMPED_WINDOW_STORE:
                return new RocksDBTimestampedWindowStore(
                    new RocksDBTimestampedSegmentedBytesStore(
                        name,
                        metricsScope(),
                        retentionPeriod,
                        segmentInterval,
                        new WindowKeySchema()),
                    retainDuplicates,
                    windowSize);
            default:
                throw new IllegalArgumentException("invalid window store type: " + windowStoreType);
        }
    }

    @Override
    public String metricsScope() {
        return "rocksdb-window";
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

    @Override
    public String toString() {
        return "RocksDbWindowBytesStoreSupplier{" +
                   "name='" + name + '\'' +
                   ", retentionPeriod=" + retentionPeriod +
                   ", segmentInterval=" + segmentInterval +
                   ", windowSize=" + windowSize +
                   ", retainDuplicates=" + retainDuplicates +
                   ", windowStoreType=" + windowStoreType +
                   '}';
    }
}
