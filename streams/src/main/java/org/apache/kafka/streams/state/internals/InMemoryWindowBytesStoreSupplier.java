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

public class InMemoryWindowBytesStoreSupplier implements WindowBytesStoreSupplier {
    private final String name;
    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    public InMemoryWindowBytesStoreSupplier(final String name,
                                            final long retentionPeriod,
                                            final long windowSize,
                                            final boolean retainDuplicates) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        return new InMemoryWindowStore(name,
                                       retentionPeriod,
                                       windowSize,
                                       retainDuplicates,
                                       metricsScope());
    }

    @Override
    public String metricsScope() {
        return "in-memory-window-state";
    }

    @Deprecated
    @Override
    public int segments() {
        throw new IllegalStateException("Segments is deprecated and should not be called");
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }


    @Override
    public long windowSize() {
        return windowSize;
    }

    // In-memory window store is not *really* segmented, so just say size is 1 ms
    @Override
    public long segmentIntervalMs() {
        return 1;
    }

    @Override
    public boolean retainDuplicates() {
        return retainDuplicates;
    }
}
